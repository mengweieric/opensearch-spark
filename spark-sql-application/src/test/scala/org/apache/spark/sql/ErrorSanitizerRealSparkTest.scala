/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.common.model.FlintStatement
import org.opensearch.flint.core.logging.{CustomLogging, ExceptionMessages, OperationMessage}
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.ErrorSanitizer.ErrorCode
import org.apache.spark.sql.test.SharedSparkSessionBase

/**
 * Companion to [[ErrorSanitizerTest]] and the redaction tests in [[FlintREPLTest]], which build
 * the analysis/parse/runtime exceptions by hand. This suite instead provokes the real Spark 3.5.1
 * exceptions from a live [[SparkSession]] so the sanitizer is exercised against the exact message
 * shapes Spark actually emits (including the appended logical plan and the {@code == SQL ==}
 * fragment) rather than a reconstruction of them. It routes each through both the sanitizer
 * directly and the full [[FlintREPL.processQueryException]] sink so the persisted error JSON is
 * asserted end to end.
 */
class ErrorSanitizerRealSparkTest
    extends QueryTest
    with SharedSparkSessionBase
    with Matchers
    with MockitoSugar {

  /** Runs a statement expected to fail and returns the root cause, mirroring the driver path. */
  private def failWith(sql: String): Throwable = {
    val raised = intercept[Throwable](spark.sql(sql).collect())
    FlintREPL.getRootCause(raised)
  }

  private def persistedJson(t: Throwable): String =
    FlintREPL.processQueryException(t, mock[FlintStatement])

  /**
   * Invokes the real production [[CustomLogging.constructLogEventMap]] (the deterministic method
   * that assembles every field a log event carries, including `body.message`, `exception.type`,
   * and `exception.message`). It is `protected` and lives in another package, so it is reached by
   * reflection rather than intercepting a live Log4j appender; this exercises the exact
   * production code path without the brittleness of appender capture.
   */
  private def constructLogEventMap(
      content: Object,
      throwable: Throwable): java.util.Map[String, Object] = {
    val method = classOf[CustomLogging].getDeclaredMethod(
      "constructLogEventMap",
      classOf[String],
      classOf[Object],
      classOf[Throwable])
    method.setAccessible(true)
    method
      .invoke(null, "ERROR", content, throwable)
      .asInstanceOf[java.util.Map[String, Object]]
  }

  test("real unresolved-column AnalysisException: only the catalog errorClass survives") {
    spark.sql("SELECT 1 AS id, 'x' AS name").createOrReplaceTempView("real_probe_vals")
    val column = "nonexistent_col_realspark_5551"
    val analysis = failWith(s"SELECT $column FROM real_probe_vals")

    // Precondition: the real message appends the resolved logical plan tree and names the column.
    analysis.getMessage should include("Project")
    analysis.getMessage should include(column)
    analysis.isInstanceOf[AnalysisException] shouldBe true

    val sanitized = ErrorSanitizer.sanitizedMessage(analysis)
    // The plan tree and its node names are gone ...
    sanitized should not include "Project"
    sanitized should not include "SubqueryAlias"
    sanitized should not include "LocalRelation"
    sanitized should not include "View"
    // ... and so is every query-derived value: the offending column name, the "Did you mean"
    // suggestion list, and the line/position text. The strict policy emits only the stable,
    // catalog-derived errorClass (an UNRESOLVED_COLUMN family constant), never getSimpleMessage.
    sanitized should not include column
    sanitized should not include "Did you mean"
    sanitized should not include "pos "
    sanitized should include("UNRESOLVED_COLUMN")

    ErrorSanitizer.classify(analysis).errorCode shouldBe ErrorCode.QueryAnalysisError

    val json = persistedJson(analysis)
    json should not include "Project"
    json should not include column
    json should include(""""errorCode":"QUERY_ANALYSIS_ERROR"""")
    json should include(
      """"exception.type":"org.apache.spark.sql.catalyst.ExtendedAnalysisException"""")
  }

  test("real ParseException: only the catalog errorClass survives, no query text or position") {
    val queryCanary = "bogus_realspark_6662"
    val parse = failWith(s"SELCT $queryCanary FROM real_probe_vals")

    parse.getMessage should include("== SQL ==")
    parse.getMessage should include(queryCanary)

    val sanitized = ErrorSanitizer.sanitizedMessage(parse)
    sanitized should not include "== SQL =="
    sanitized should not include queryCanary
    // The free-text syntax diagnostic and the line/position text are query-derived and gone; only
    // the stable catalog errorClass (and its sqlState) remain.
    sanitized should not include "Syntax error"
    sanitized should not include "pos "
    sanitized should include("PARSE_SYNTAX_ERROR")

    ErrorSanitizer.classify(parse).errorCode shouldBe ErrorCode.QuerySyntaxError

    val json = persistedJson(parse)
    json should not include queryCanary
    json should include(""""errorCode":"QUERY_SYNTAX_ERROR"""")
  }

  test("real runtime SparkThrowable: only errorClass and sqlState survive, no query fragment") {
    spark.conf.set("spark.sql.ansi.enabled", "true")
    val runtime =
      try failWith("SELECT 1 / 0 AS x")
      finally spark.conf.unset("spark.sql.ansi.enabled")

    // Precondition: the real ANSI arithmetic failure appends the offending SQL after a newline.
    runtime.getMessage should include("== SQL")
    runtime.isInstanceOf[org.apache.spark.SparkThrowable] shouldBe true

    val sanitized = ErrorSanitizer.sanitizedMessage(runtime)
    sanitized shouldBe "[DIVIDE_BY_ZERO] sqlState=[22012]"
    sanitized should not include "== SQL"
    sanitized should not include "SELECT"

    ErrorSanitizer.classify(runtime).errorCode shouldBe ErrorCode.SparkQueryError

    val json = persistedJson(runtime)
    json should not include "SELECT 1 / 0"
    json should include(""""errorCode":"SPARK_QUERY_ERROR"""")
  }

  test("getRootCause and processQueryException are cycle-safe on a self-referencing cause") {
    // A throwable whose cause is itself. Before the cycle guard, getRootCause recursed until it
    // threw StackOverflowError on the error-handling path, masking the original failure.
    val selfReferencing = new RuntimeException("cycle_realspark_7773") {
      override def getCause: Throwable = this
    }

    noException should be thrownBy FlintREPL.getRootCause(selfReferencing)
    FlintREPL.getRootCause(selfReferencing) should be theSameInstanceAs selfReferencing

    val json = persistedJson(selfReferencing)
    json should include(""""errorCode":"UNKNOWN_ERROR"""")
  }

  test(
    "CustomLogging event map for a redacted real analysis exception carries no query canaries") {
    // End-to-end proof over the actual logging construction path: a real analysis failure is
    // redacted exactly as the production catch site does, then handed to the real
    // CustomLogging.constructLogEventMap. Every field the log event exposes -- body.message,
    // exception.message, exception.type, and the rendered throwable (toString header + stack
    // trace) -- is asserted free of any query-derived content. Coverage note: this asserts the
    // fields constructLogEventMap populates and the throwable rendering log4j reads from; it does
    // not assert the downstream appender/JSON serializer, which only re-serializes these fields.
    spark.sql("SELECT 1 AS id, 'x' AS name").createOrReplaceTempView("real_probe_log")
    val column = "nonexistent_col_customlogging_8884"
    val analysis = failWith(s"SELECT $column FROM real_probe_log")

    // Reproduce the production catch-site redaction exactly (FlintREPL.redactThrowable), including
    // the "prefix: <sanitized>" message shape passed to CustomLogging.logError.
    val redacted = FlintREPL.redactThrowable(analysis)
    val errorMessage = s"${ExceptionMessages.QueryAnalysisErrorPrefix}: ${redacted.getMessage}"
    val logEventMap = constructLogEventMap(new OperationMessage(errorMessage, 400), redacted)

    val body = logEventMap.get("body").asInstanceOf[java.util.Map[String, Object]]
    val attributes = logEventMap.get("attributes").asInstanceOf[java.util.Map[String, Object]]
    val bodyMessage = body.get("message").asInstanceOf[String]
    val exceptionMessage = attributes.get("exception.message").asInstanceOf[String]
    val exceptionType = attributes.get("exception.type").asInstanceOf[String]

    // Render the throwable exactly as log4j would: the toString() header plus the stack-trace body.
    val sw = new java.io.StringWriter()
    redacted.printStackTrace(new java.io.PrintWriter(sw))
    val rendered = sw.toString

    // No query-derived content in any field the event exposes.
    Seq(bodyMessage, exceptionMessage, exceptionType, redacted.toString, rendered).foreach {
      field =>
        field should not include column
        field should not include "Project"
        field should not include "SubqueryAlias"
        field should not include "== SQL =="
        field should not include "Did you mean"
    }

    // Positive contract: the surviving content is stable and non-customer.
    bodyMessage should include(ExceptionMessages.QueryAnalysisErrorPrefix)
    exceptionMessage should include("UNRESOLVED_COLUMN") // errorClass-only sanitized message
    exceptionType shouldBe "org.apache.spark.sql.exception.RedactedException"
    // The original exception type is still recoverable from the rendered header for debugging.
    redacted.toString should include("ExtendedAnalysisException")
  }
}
