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

import org.apache.spark.SparkThrowable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.parser.ParseException

/**
 * Centralized policy for turning a throwable into (a) a message, and (b) a machine-readable
 * classification that does not depend on that message's wording.
 *
 * Rationale for splitting message from classification: an error string that is simultaneously the
 * human-readable diagnostic and the classification protocol cannot be changed safely. Redacting
 * it for compliance alters the classification; preserving it for classification blocks redaction.
 * Emitting [[classify]] alongside the message lets the message be shaped for its audience while
 * the classification stays stable.
 *
 * There are two message audiences, and they get different policies because they have different
 * risk profiles:
 *
 *   - [[customerMessage]] is what is persisted to the query result store and forwarded
 *     downstream. Customers need actionable diagnostics, so this keeps the useful single-line
 *     message (the offending identifier, the "Did you mean" suggestion, the parser detail, the
 *     first-line runtime diagnostic) while dropping the parts that echo the query itself: the
 *     appended logical plan tree, the `== SQL ==` block, and any following multi-line query
 *     context. It never appends a SQL state code.
 *   - [[operatorLogMessage]] is what reaches the driver logs. Logs are broadly readable, so this
 *     stays strict: only the stable, catalog-derived `errorClass` plus, for a generic failure
 *     with no `errorClass`, a bounded and safe cause class name. It never surfaces
 *     `getSimpleMessage`, message parameters, query context, or a logical plan. The one bounded
 *     read of `getMessage` is [[platformClassTokenFromMessage]], which extracts only an
 *     allowlisted platform class token (never the surrounding text) as a last-resort cause
 *     handle.
 *
 * Policies are per exception family because there is no single textual rule that is safe for
 * every message. Anything unrecognized falls back to the most conservative option that does not
 * regress the currently shipped behavior.
 */
object ErrorSanitizer extends Logging {

  /**
   * Message used when the sanitizer itself fails. Deliberately carries no detail from the
   * original throwable beyond the fact that something was redacted: if policy evaluation threw,
   * we cannot assert anything about what the original message contains, so returning it would be
   * fail-open.
   */
  private[sql] val RedactedFallbackMessage = "error details were redacted"

  /** Label emitted for a Spark-native failure that reports no catalog error class. */
  private[sql] val GenericSparkErrorLabel = "SPARK_ERROR"

  /** Upper bound on a cause class name emitted into the operator log, as defense in depth. */
  private val MaxCauseClassNameLength = 256

  /**
   * Fully-qualified class tokens that are safe to surface in the operator log because their
   * package belongs to the platform, not to customer or application code. Used only to recover a
   * cause class name from message text when no structured cause is available. Each alternative is
   * matched as a package prefix (it must be followed by a `.` and cannot be embedded inside
   * another qualified identifier), the token must end in a segment terminating in `Exception` or
   * `Error`, and only the token itself is ever emitted -- never the surrounding text. An
   * arbitrary customer or application package token does not match and is therefore rejected.
   */
  private val PlatformExceptionClassPattern =
    ("(?<![A-Za-z0-9_$.])(?:javax|java|scala|org\\.apache\\.spark|org\\.opensearch)" +
      "(?:\\.[A-Za-z_$][A-Za-z0-9_$]*)*" +
      "\\.[A-Za-z_$][A-Za-z0-9_$]*(?:Exception|Error)").r

  /**
   * Closed vocabulary of error codes. A fixed enumeration is what makes this channel trustworthy:
   * nothing is interpolated into it, so it provably carries no customer content, and adding or
   * changing a value is an explicit, reviewable API change rather than an incidental side effect
   * of editing a message string.
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
   *   of message text: a status recovered by regex from a string we also rewrite would
   *   reintroduce the coupling this class exists to remove.
   */
  case class ErrorClassification(errorCode: String, statusCode: Option[Int])

  /**
   * Classifies a throwable using typed fields only. Callers are expected to have unwrapped to the
   * root cause already; this additionally walks the cause chain so a wrapped typed failure is
   * still recognized.
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
   * Returns the message persisted to the query result store and forwarded downstream, with the
   * query itself removed but the actionable diagnostic kept.
   *
   * Per-family policy:
   *   - [[AnalysisException]] (including `ExtendedAnalysisException` and [[ParseException]]): the
   *     first line of `getSimpleMessage`. `getSimpleMessage` is Spark's own "message without the
   *     logical plan", and for a [[ParseException]] it also excludes the appended `== SQL ==`
   *     block (both live only in `getMessage`). Taking the first line additionally drops any
   *     trailing multi-line query context. What remains is the diagnostic a customer needs: the
   *     offending identifier, the "Did you mean" suggestion, and the parser detail. No SQL state
   *     is appended.
   *   - Any other [[SparkThrowable]] (e.g. a `SparkException` internal error, an ANSI runtime
   *     failure): the first line of the rendered message. This preserves the leading diagnostic
   *     that some downstream consumers still classify on (for example the "Cannot resolve the
   *     runtime replaceable expression ..." phrase) while dropping the appended `== SQL ==` query
   *     fragment and any following context, which live below the first line. No SQL state is
   *     appended.
   *   - [[OpenSearchBulkWriteException]]: its own message, which is assembled from structured
   *     fields and never includes per-item failure text.
   *   - [[AmazonS3Exception]]: structured service/status/error-code fields rather than the raw
   *     message, which can embed customer bucket and key names.
   *   - Anything else, including other AWS exceptions: the first line only. Their message is
   *     frequently an already-curated, safe sentence (`processQueryException` replaces a Glue
   *     access-denied message with an actionable explanation), and reducing it to a code would
   *     discard that. This is a floor, not a guarantee, since a single-line message can still
   *     carry customer values; broadening redaction there is a separate, reviewed change.
   */
  def customerMessage(t: Throwable): String = try {
    customerPolicyMessage(t)
  } catch {
    case NonFatal(e) =>
      // Fail closed, and log class names only. Policy evaluation failing means we cannot assert
      // what the original message contains, so it is neither returned nor logged.
      logError(
        s"Failed to sanitize throwable of type [${t.getClass.getName}] " +
          s"(sanitizer error type [${e.getClass.getName}]); returning redacted message")
      RedactedFallbackMessage
  }

  /**
   * Returns the message written to the driver logs, redacted to the stable classification only.
   * The whole cause chain is inspected so an enclosing wrapper cannot hide the failure it
   * carries: `processQueryException` unwraps to the root cause for the customer message and
   * classification, but hands the original throwable here so a wrapper (for example a
   * task-failure `SparkException` around a non-Spark cause) still routes to the strict label
   * instead of the raw first-line floor.
   *
   * Resolution order over the chain:
   *   - The structured fields of the first [[OpenSearchBulkWriteException]] or
   *     [[AmazonS3Exception]] found, assembled from typed fields and carrying no query content.
   *   - Otherwise, if anything in the chain is a [[SparkThrowable]] (including
   *     [[AnalysisException]], `ExtendedAnalysisException`, and [[ParseException]]): a bracketed
   *     catalog `errorClass` when present, otherwise the bare `[SPARK_ERROR]` label plus, for a
   *     generic failure with no `errorClass`, a single bounded and safe cause class name as
   *     `cause=[...]`. That cause name is taken from the structured immediate cause, or, only
   *     when none exists, from an allowlisted platform class token in the message text; no other
   *     message content is ever read.
   *   - Otherwise the first line of the deepest cause, matching the shipped floor for a chain
   *     with no recognized type.
   */
  def operatorLogMessage(t: Throwable): String = try {
    operatorPolicyMessage(t)
  } catch {
    case NonFatal(e) =>
      logError(
        s"Failed to build operator log message for throwable of type [${t.getClass.getName}] " +
          s"(sanitizer error type [${e.getClass.getName}]); returning redacted message")
      RedactedFallbackMessage
  }

  private def customerPolicyMessage(t: Throwable): String = t match {
    case be: OpenSearchBulkWriteException => openSearchBulkWriteMessage(be)
    case s3: AmazonS3Exception => s3StructuredMessage(s3)
    // AnalysisException and its subtypes must be matched before the generic SparkThrowable case
    // below, since they are themselves SparkThrowables but have a plan-free message accessor.
    case ae: AnalysisException => firstLine(ae.getSimpleMessage)
    case _: SparkThrowable => firstLine(t.getMessage)
    case other => firstLine(other.getMessage)
  }

  /**
   * Walks the cause chain so a wrapper cannot hide the failure it carries or downgrade it to the
   * raw first-line floor, mirroring [[classify]]. See [[operatorLogMessage]] for the resolution
   * order. The floor reads the deepest cause's message, which is the throwable the customer
   * message and classification are built from, so a chain with no recognized type logs the same
   * first line it did before wrappers were preserved for this channel.
   */
  private def operatorPolicyMessage(t: Throwable): String = {
    val chain = causeChain(t)
    chain
      .collectFirst {
        case be: OpenSearchBulkWriteException => openSearchBulkWriteMessage(be)
        case s3: AmazonS3Exception => s3StructuredMessage(s3)
      }
      .orElse {
        if (chain.exists(_.isInstanceOf[SparkThrowable])) Some(sparkThrowableLogLabel(t))
        else None
      }
      .getOrElse(firstLine(chain.last.getMessage))
  }

  private def s3StructuredMessage(s3: AmazonS3Exception): String =
    s"serviceName=[${s3.getServiceName}], statusCode=[${s3.getStatusCode}], " +
      s"errorCode=[${s3.getErrorCode}]"

  /**
   * Returns the safe structured bulk-write message plus a temporary, canonical compatibility
   * token for existing downstream authorization translations. The token is derived only from
   * typed status and exception-name fields; no OpenSearch failure reason is copied. Legacy
   * consumers may require the exact `type=security_exception, reason=OpenSearch exception
   * [type=authorization_exception` shape in addition to
   * `exception.type=java.lang.RuntimeException`. Remove this token together with the
   * persisted-type shim once consumers classify on `errorCode` and `statusCode`.
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
   * The strict operator-log label for a Spark-native failure.
   *
   *   - When the chain carries a catalog `errorClass`, that class alone is the label. It is drawn
   *     from Spark's error-conditions catalog, carries no query content, and is itself the stable
   *     identifier, so nothing further is appended.
   *   - For a generic Spark failure that reports no `errorClass`, the bare
   *     [[GenericSparkErrorLabel]] is emitted plus, when one can be obtained safely, a single
   *     bounded cause class name so an operator keeps a debugging handle. The cause class comes
   *     first from the structured immediate cause ([[safeCauseClassName]]); only when there is
   *     none is a strictly-allowlisted platform class token recovered from the message text
   *     ([[platformClassTokenFromMessage]]). No other message text is read.
   */
  private def sparkThrowableLogLabel(t: Throwable): String =
    firstErrorClass(t) match {
      case Some(errorClass) =>
        s"[$errorClass]"
      case None =>
        safeCauseClassName(t).orElse(platformClassTokenFromMessage(t)) match {
          case Some(causeName) => s"[$GenericSparkErrorLabel] cause=[$causeName]"
          case None => s"[$GenericSparkErrorLabel]"
        }
    }

  /**
   * First non-empty catalog error class in the cause chain, reading the typed field defensively.
   */
  private def firstErrorClass(t: Throwable): Option[String] =
    causeChain(t).flatMap {
      case st: SparkThrowable =>
        try Option(st.getErrorClass).filter(_.nonEmpty)
        catch { case NonFatal(_) => None }
      case _ => None
    }.headOption

  /**
   * The immediate cause's class name, bounded and guarded. A class name carries no customer
   * content, so it is safe to log, but the accessors are still guarded: a hostile `getCause` that
   * throws, or a self-referencing cause, yields None rather than a secondary failure or an
   * unbounded token.
   */
  private def safeCauseClassName(t: Throwable): Option[String] = {
    val cause =
      try t.getCause
      catch { case NonFatal(_) => null }
    if (cause == null || cause.eq(t)) {
      None
    } else {
      try Some(boundName(cause.getClass.getName))
      catch { case NonFatal(_) => None }
    }
  }

  private def boundName(name: String): String =
    if (name.length <= MaxCauseClassNameLength) name
    else name.substring(0, MaxCauseClassNameLength)

  /**
   * A best-effort, strictly-bounded platform exception class token recovered from a throwable's
   * message text, used only as a fallback when no structured cause class is available -- the
   * review case where a generic `SparkException` names the underlying platform exception only in
   * its message. This is the single place the operator-log channel reads `getMessage`, and it
   * never emits surrounding text: it returns only a fully-qualified class token that begins with
   * an allowlisted platform package prefix, ends in `Exception`/`Error`, and is bounded to
   * [[MaxCauseClassNameLength]] characters (see [[PlatformExceptionClassPattern]]). `getMessage`
   * is read under a guard so a hostile override cannot propagate a secondary failure, and an
   * arbitrary customer or application package token is rejected because it does not match the
   * allowlist. Returns None when nothing matches, which is the common case.
   */
  private def platformClassTokenFromMessage(t: Throwable): Option[String] = {
    val message =
      try Option(t.getMessage).getOrElse("")
      catch { case NonFatal(_) => "" }
    PlatformExceptionClassPattern.findFirstIn(message).map(boundName)
  }

  private def firstLine(message: String): String =
    Option(message).getOrElse("").split("\n", 2)(0)

  private def causeChain(t: Throwable): Stream[Throwable] = {
    def loop(current: Throwable, seen: Set[Throwable]): Stream[Throwable] = {
      if (current == null || seen.contains(current)) {
        Stream.empty
      } else {
        // getCause runs on the error-handling path; a hostile throwable whose getCause() throws
        // must not propagate a secondary failure. Treat a throwing getCause() as "no further cause".
        val next =
          try current.getCause
          catch { case NonFatal(_) => null }
        current #:: loop(next, seen + current)
      }
    }
    loop(t, Set.empty)
  }
}
