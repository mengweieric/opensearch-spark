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
import org.apache.spark.SparkThrowableHelper
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.exception.RedactedException

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
 *     stays strict: it never surfaces `getSimpleMessage`, an interpolated message, message
 *     parameter values, query context, or a logical plan. It keeps the maximum diagnostic detail
 *     that is provably not customer-derived: the stable, catalog-derived `errorClass`; the static
 *     message *template* for that class, taken from Spark's error-conditions catalog with its
 *     `<param>` placeholders left un-interpolated so no parameter value is ever substituted in;
 *     and a bounded, safe cause class name. For a generic failure with no `errorClass` it emits
 *     the bare `[SPARK_ERROR]` label plus that cause class. It never appends a SQL state code.
 *     The one bounded read of `getMessage` is [[platformClassTokenFromMessage]], used only when
 *     no structured cause exists, which extracts only an allowlisted platform class token (never
 *     the surrounding text) as a last-resort cause handle.
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

  /**
   * Label emitted for a failure whose cause chain carries no recognized type -- no structured
   * bulk-write or S3 failure and no [[SparkThrowable]]. The shipped floor logged the deepest
   * cause's first message line, but for a query failure that line is customer-derived (it can
   * carry the offending identifier, a literal, or an OpenSearch document value), so it must not
   * reach the broadly-readable driver log. This label pairs with the deepest cause's bounded,
   * safe class name instead, giving an operator a type handle with no message text.
   */
  private[sql] val UnknownErrorLabel = "UNKNOWN_ERROR"

  /** Upper bound on a cause class name emitted into the operator log, as defense in depth. */
  private val MaxCauseClassNameLength = 256

  /**
   * Upper bound on the static catalog message template emitted into the operator log. The bundled
   * templates are short, so this is defense in depth against a pathological future catalog entry
   * rather than an expected truncation.
   */
  private val MaxTemplateLength = 1024

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
   *     catalog `errorClass` when present, followed by that class's static catalog message
   *     template (placeholders left un-interpolated, carrying no parameter value), otherwise the
   *     bare `[SPARK_ERROR]` label. In either case a single bounded and safe cause class name is
   *     appended as `cause=[...]` when one is available. That cause name is taken from the
   *     structured immediate cause, or, only for a generic failure with no `errorClass` and no
   *     structured cause, from an allowlisted platform class token in the message text; no other
   *     message content is ever read.
   *   - Otherwise, for a chain with no recognized type, a bare [[UnknownErrorLabel]] plus the
   *     deepest cause's bounded, safe class name ([[unrecognizedThrowableLogLabel]]). No message
   *     text is read, so the customer-derived first line the shipped floor logged never appears.
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

  /**
   * Wraps a throwable so that only its [[operatorLogMessage]] is ever exposed via `getMessage`,
   * `getLocalizedMessage`, `toString`, and the rendered stack-trace header, while preserving the
   * original exception type name and frames for debugging. This is the single redaction point for
   * the driver logs: a redacted throwable can be handed to any logger (directly, or via
   * [[org.apache.spark.sql.util.ThrowableHandler]]) without the original message text, query
   * context, or logical plan leaking through the logged exception. The persisted / forwarded
   * error string is built separately from [[customerMessage]].
   */
  def redactThrowable(t: Throwable): Throwable = {
    val redacted = new RedactedException(t.getClass.getName, operatorLogMessage(t))
    redacted.setStackTrace(t.getStackTrace)
    redacted
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
   * order. When no recognized type is found, the floor emits a bare label plus the deepest
   * cause's safe class name via [[unrecognizedThrowableLogLabel]] rather than reading any message
   * text, so a customer-derived first line can never reach the driver log.
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
      .getOrElse(unrecognizedThrowableLogLabel(chain))
  }

  /**
   * The operator-log label for a cause chain that carries no recognized type. Emits only the bare
   * [[UnknownErrorLabel]] plus a bounded, safe class name for the deepest cause -- a code
   * identifier, never a customer value, consistent with [[safeCauseClassName]]. Unlike the
   * shipped floor it reads no message text, so an offending identifier, a literal, or an
   * OpenSearch document value that a query failure's message can carry never reaches the log.
   * `getClass` is read under a guard so even a pathological throwable yields the bare label
   * rather than a secondary failure.
   */
  private def unrecognizedThrowableLogLabel(chain: Stream[Throwable]): String = {
    val deepest = chain.last
    val className =
      try Some(boundName(deepest.getClass.getName))
      catch { case NonFatal(_) => None }
    className match {
      case Some(name) => s"[$UnknownErrorLabel] type=[$name]"
      case None => s"[$UnknownErrorLabel]"
    }
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
   *   - When the chain carries a catalog `errorClass`, that class is the leading token, followed
   *     by the static message *template* for the class when one resolves. The template is read
   *     from Spark's error-conditions catalog via [[catalogMessageTemplate]] with its `<param>`
   *     placeholders left un-interpolated, so it is provably free of parameter values, the
   *     offending identifier, the suggestion, any literal, and the query. It is the maximum
   *     diagnostic detail that is not customer-derived. When the class has no resolvable template
   *     (an unknown or malformed class) only the bracketed class is emitted, matching the
   *     previously shipped behavior.
   *   - For a generic Spark failure that reports no `errorClass`, the bare
   *     [[GenericSparkErrorLabel]] is emitted.
   *
   * In both cases a single bounded, safe cause class name is appended as `cause=[...]` when one
   * is available. It comes first from the structured immediate cause ([[safeCauseClassName]]);
   * only for the generic no-`errorClass` path, and only when there is no structured cause, is a
   * strictly-allowlisted platform class token recovered from the message text
   * ([[platformClassTokenFromMessage]]). No other message text is read. A cause class name is a
   * code identifier, never a customer value, so it is safe to log; for an opaque `INTERNAL_ERROR`
   * (whose template is only the `<message>` placeholder) it is often the one useful handle.
   */
  private def sparkThrowableLogLabel(t: Throwable): String =
    firstErrorClass(t) match {
      case Some(errorClass) =>
        val label = new StringBuilder("[").append(errorClass).append("]")
        catalogMessageTemplate(errorClass).foreach(template => label.append(" ").append(template))
        safeCauseClassName(t).foreach(cause => label.append(" cause=[").append(cause).append("]"))
        label.toString
      case None =>
        safeCauseClassName(t).orElse(platformClassTokenFromMessage(t)) match {
          case Some(causeName) => s"[$GenericSparkErrorLabel] cause=[$causeName]"
          case None => s"[$GenericSparkErrorLabel]"
        }
    }

  /**
   * The static catalog message template for an error class, normalized for a single log line, or
   * None when no safe template can be produced.
   *
   * The template is read from Spark's bundled error-conditions catalog
   * ([[org.apache.spark.SparkThrowableHelper.errorReader]]), which keeps the authored `<param>`
   * placeholders literal and does not substitute the throwable's parameter values. It is
   * therefore derived entirely from the platform's static resources and is independent of the
   * specific failure: no customer identifier, suggestion, literal, operand, or query text can
   * appear in it. `getMessageTemplate` throws for an unknown or malformed class name (a custom
   * throwable can return an arbitrary `errorClass`), so the lookup is guarded and yields None in
   * that case rather than emitting anything unvalidated. Newlines and repeated whitespace are
   * collapsed to single spaces so the template cannot introduce extra log lines, and the result
   * is length-bounded as defense in depth.
   */
  private def catalogMessageTemplate(errorClass: String): Option[String] =
    try {
      Option(SparkThrowableHelper.errorReader.getMessageTemplate(errorClass))
        .map(template => boundTemplate(template.replaceAll("\\s+", " ").trim))
        .filter(_.nonEmpty)
    } catch {
      case NonFatal(_) => None
    }

  private def boundTemplate(template: String): String =
    if (template.length <= MaxTemplateLength) template
    else template.substring(0, MaxTemplateLength)

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
