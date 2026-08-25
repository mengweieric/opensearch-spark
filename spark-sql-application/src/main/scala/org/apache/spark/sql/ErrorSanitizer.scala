/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import scala.util.control.NonFatal

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.glue.model.{AccessDeniedException => GlueAccessDeniedException, AWSGlueException}
import com.amazonaws.services.s3.model.AmazonS3Exception
import org.opensearch.flint.core.storage.OpenSearchBulkWriteException

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.parser.ParseException

/**
 * Centralized policy for turning a throwable into (a) a message that is safe to log and forward
 * downstream, and (b) a machine-readable classification that does not depend on that message's
 * wording.
 *
 * Rationale for splitting the two: an error string that is simultaneously the human-readable
 * diagnostic and the classification protocol cannot be changed safely. Redacting it for compliance
 * alters the classification; preserving it for classification blocks redaction. Emitting
 * [[classify]] alongside [[sanitizedMessage]] lets the message be rewritten freely while
 * classification stays stable.
 *
 * Policies are per exception family because there is no single textual rule that is safe for every
 * message. Each family below is handled by what its type actually guarantees, and anything
 * unrecognized falls back to the most conservative option that does not regress the currently
 * shipped behavior.
 */
object ErrorSanitizer extends Logging {

  /**
   * Message used when the sanitizer itself fails. Deliberately carries no detail from the original
   * throwable beyond its type: if policy evaluation threw, we cannot assert anything about what the
   * original message contains, so returning it would be fail-open.
   */
  private[sql] val RedactedFallbackMessage = "error details were redacted"

  /**
   * Closed vocabulary of error codes. A fixed enumeration is what makes this channel trustworthy:
   * nothing is interpolated into it, so it provably carries no customer content, and adding or
   * changing a value is an explicit, reviewable API change rather than an incidental side effect of
   * editing a message string.
   */
  object ErrorCode {
    val QuerySyntaxError = "QUERY_SYNTAX_ERROR"
    val QueryAnalysisError = "QUERY_ANALYSIS_ERROR"
    val SparkQueryError = "SPARK_QUERY_ERROR"
    val S3Error = "S3_ERROR"
    val GlueError = "GLUE_ERROR"
    val GlueAccessDenied = "GLUE_ACCESS_DENIED"
    val OpenSearchWriteError = "OPENSEARCH_WRITE_ERROR"
    val OpenSearchWriteAccessDenied = "OPENSEARCH_WRITE_ACCESS_DENIED"
    val Unknown = "UNKNOWN_ERROR"
  }

  /**
   * Machine-readable classification of a failure.
   *
   * @param errorCode
   *   a value from [[ErrorCode]]
   * @param statusCode
   *   HTTP status when the failure carries one on a typed field, otherwise None. Never parsed out
   *   of message text: a status recovered by regex from a string we also rewrite would reintroduce
   *   the coupling this class exists to remove.
   */
  case class ErrorClassification(errorCode: String, statusCode: Option[Int])

  /**
   * Classifies a throwable using typed fields only. Callers are expected to have unwrapped to the
   * root cause already; this additionally walks the cause chain so a wrapped typed failure is still
   * recognized.
   *
   * Specific failures win over generic ones regardless of nesting depth. A write or connector
   * failure surfaces to the driver wrapped in a task-failure [[SparkException]], so matching the
   * outermost throwable first would classify every such failure as a generic Spark error and lose
   * the actionable status.
   */
  def classify(t: Throwable): ErrorClassification = try {
    val chain = causeChain(t)
    chain
      .flatMap(classifySpecific)
      .headOption
      .orElse(chain.flatMap(classifyGeneric).headOption)
      .getOrElse(ErrorClassification(ErrorCode.Unknown, None))
  } catch {
    case NonFatal(e) =>
      // Log class names only. The subject throwable's message may carry customer content, and the
      // sanitizer's own failure `e` can wrap it, so neither is passed to the logger.
      logError(
        s"Failed to classify throwable of type [${t.getClass.getName}] " +
          s"(sanitizer error type [${e.getClass.getName}]); defaulting to unknown")
      ErrorClassification(ErrorCode.Unknown, None)
  }

  /**
   * Failures that carry an actionable, attributable status on a typed field. These are the cases
   * where classification must not be lost to an enclosing wrapper.
   */
  private def classifySpecific(t: Throwable): Option[ErrorClassification] = t match {
    case be: OpenSearchBulkWriteException =>
      val code =
        if (isOpenSearchAuthorizationFailure(be)) ErrorCode.OpenSearchWriteAccessDenied
        else ErrorCode.OpenSearchWriteError
      Some(ErrorClassification(code, Some(be.getStatusCode)))
    case s3: AmazonS3Exception =>
      Some(ErrorClassification(ErrorCode.S3Error, Some(s3.getStatusCode)))
    case glueDenied: GlueAccessDeniedException =>
      // Glue signals authorization failures with the concrete AccessDeniedException type but an SDK
      // HTTP status of 400, so the status alone would misclassify it. Match the type and preserve
      // whatever status the SDK reported rather than asserting 403.
      Some(ErrorClassification(ErrorCode.GlueAccessDenied, Some(glueDenied.getStatusCode)))
    case glue: AWSGlueException =>
      val code =
        if (glue.getStatusCode == 403) ErrorCode.GlueAccessDenied else ErrorCode.GlueError
      Some(ErrorClassification(code, Some(glue.getStatusCode)))
    case ase: AmazonServiceException =>
      Some(ErrorClassification(ErrorCode.Unknown, Some(ase.getStatusCode)))
    case _ => None
  }

  private def isOpenSearchAuthorizationFailure(be: OpenSearchBulkWriteException): Boolean =
    be.getStatusCode == 403 && be.getExceptionTypeNames.contains("security_exception")

  /** Query-lifecycle families, used only when nothing more specific exists in the chain. */
  private def classifyGeneric(t: Throwable): Option[ErrorClassification] = t match {
    case _: ParseException =>
      Some(ErrorClassification(ErrorCode.QuerySyntaxError, None))
    case _: AnalysisException =>
      Some(ErrorClassification(ErrorCode.QueryAnalysisError, None))
    // Any remaining Spark-native failure (SparkException and the SparkThrowable runtime-error
    // family, e.g. SparkArithmeticException) is a query error. Kept after the two more specific
    // cases above, which are themselves SparkThrowables.
    case _: SparkThrowable =>
      Some(ErrorClassification(ErrorCode.SparkQueryError, None))
    case _ => None
  }

  /**
   * Returns a message with customer query content removed.
   *
   * Per-family policy:
   *   - [[AnalysisException]] (including `ExtendedAnalysisException` and `ParseException`):
   *     `getSimpleMessage`, which Spark documents as the diagnostic without the appended logical
   *     plan or raw SQL.
   *   - [[OpenSearchBulkWriteException]]: its own message, which is assembled from structured
   *     fields and never includes per-item failure text.
   *   - [[AmazonS3Exception]]: structured service/status/error-code fields rather than the raw
   *     message, matching the policy already applied in `ExceptionMessages.redactMessage`.
   *   - [[SparkThrowable]] (other than [[AnalysisException]]): only the stable `errorClass`
   *     identifier and, when present, the `sqlState`. Neither `getMessage` nor the message
   *     parameters are read: the rendered message interpolates parameter values (literals,
   *     identifiers, paths, and the appended `== SQL ==` query fragment), so any of it can carry
   *     customer content. `errorClass` and `sqlState` are drawn from Spark's error-conditions
   *     catalog and contain no query content.
   *   - Anything else, including other AWS exceptions: the first line only.
   *
   * Other AWS families deliberately keep their first line rather than being reduced to structured
   * fields. Their message is frequently already a curated, safe sentence -- `processQueryException`
   * replaces a Glue access-denied message with an actionable explanation -- and reducing it to an
   * error code would discard that. Broadening redaction there changes text that downstream
   * consumers may classify on, so it belongs in its own reviewed change rather than riding along
   * with this one.
   *
   * On the final case: the stricter option is a type plus a generic message, and that is the right
   * end state. It is not enabled yet because consumers still classify some failures by matching
   * message text, so replacing every unrecognized message with a constant would break more
   * classifications than it protects. Keeping the first line holds the current behavior while the
   * structured [[classify]] channel is adopted; it is a floor, not a guarantee, since a single-line
   * message can still contain customer values.
   */
  def sanitizedMessage(t: Throwable): String = try {
    policyMessage(t)
  } catch {
    case NonFatal(e) =>
      // Fail closed, and log class names only. Policy evaluation failing means we cannot assert
      // what the original message contains, so it is neither returned nor logged; the sanitizer's
      // own failure `e` may itself wrap that message, so only its type is logged.
      logError(
        s"Failed to sanitize throwable of type [${t.getClass.getName}] " +
          s"(sanitizer error type [${e.getClass.getName}]); returning redacted message")
      RedactedFallbackMessage
  }

  private def policyMessage(t: Throwable): String = t match {
    case ae: AnalysisException => ae.getSimpleMessage
    case be: OpenSearchBulkWriteException => openSearchBulkWriteMessage(be)
    case s3: AmazonS3Exception =>
      s"serviceName=[${s3.getServiceName}], statusCode=[${s3.getStatusCode}], " +
        s"errorCode=[${s3.getErrorCode}]"
    case st: SparkThrowable => sparkThrowableMessage(st)
    case other => firstLine(other.getMessage)
  }

  /**
   * Returns the safe structured bulk-write message plus a temporary, canonical compatibility token
   * for existing downstream authorization translations. The token is derived only from typed status
   * and exception-name fields; no OpenSearch failure reason is copied. Legacy consumers may require
   * the exact `type=security_exception, reason=OpenSearch exception [type=authorization_exception`
   * shape in addition to `exception.type=java.lang.RuntimeException`. Remove this token together
   * with the persisted-type shim once consumers classify on `errorCode` and `statusCode`.
   */
  private def openSearchBulkWriteMessage(be: OpenSearchBulkWriteException): String = {
    val message = firstLine(be.getMessage)
    val isAuthorizationFailure = isOpenSearchAuthorizationFailure(be)
    if (isAuthorizationFailure) {
      s"$message, type=security_exception, " +
        "reason=OpenSearch exception [type=authorization_exception]"
    } else {
      message
    }
  }

  /**
   * For a [[SparkThrowable]] that is not an [[AnalysisException]], emit only the stable
   * `errorClass` and, when present, the `sqlState`. `getMessage` is never called and message
   * parameters are never read: both interpolate customer values (literals, identifiers, paths, and
   * the `== SQL ==` query fragment). `errorClass` and `sqlState` come from Spark's error-conditions
   * catalog and carry no query content. A throwable that reports no `errorClass` (legacy,
   * non-catalog Spark failures) yields a bare label so nothing derived from the message leaks.
   */
  private def sparkThrowableMessage(st: SparkThrowable): String = {
    val label = Option(st.getErrorClass).filter(_.nonEmpty).getOrElse("SPARK_ERROR")
    Option(st.getSqlState).filter(_.nonEmpty) match {
      case Some(sqlState) => s"[$label] sqlState=[$sqlState]"
      case None => s"[$label]"
    }
  }

  private def firstLine(message: String): String =
    Option(message).getOrElse("").split("\n", 2)(0)

  private def causeChain(t: Throwable): Stream[Throwable] = {
    def loop(current: Throwable, seen: Set[Throwable]): Stream[Throwable] = {
      if (current == null || seen.contains(current)) Stream.empty
      else current #:: loop(current.getCause, seen + current)
    }
    loop(t, Set.empty)
  }
}
