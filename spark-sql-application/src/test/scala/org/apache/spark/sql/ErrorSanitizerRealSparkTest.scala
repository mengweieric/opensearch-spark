/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
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

  // --------------------------------------------------------------------------
  // Production JSON serializer coverage + table-driven real-exception canary
  // matrix.
  //
  // The tests above assert the intermediate field map. The tests below drive
  // the exact string log4j receives -- CustomLogging.convertToJson applied to
  // the map constructLogEventMap builds -- then parse that line and recursively
  // scan every leaf value, so a canary that survived only after JSON escaping
  // (a CRLF or Unicode canary) is still caught. The matrix provokes a broad set
  // of real Spark 3.5.1 failures and routes each through the sanitizer, the
  // full FlintREPL.processQueryException sink, and the production serializer.
  // --------------------------------------------------------------------------

  private val jsonMapper = new ObjectMapper()

  /**
   * The exact line log4j is handed: constructLogEventMap -> convertToJson, both reached by
   * reflection. convertToJson is `private static` and is the only place the OTEL log map becomes
   * the emitted string, so asserting on its output covers the serializer itself rather than just
   * the map it serializes.
   */
  private def productionLogLine(content: Object, throwable: Throwable): String = {
    val map = constructLogEventMap(content, throwable)
    val method =
      classOf[CustomLogging].getDeclaredMethod("convertToJson", classOf[java.util.Map[_, _]])
    method.setAccessible(true)
    method.invoke(null, map).asInstanceOf[String]
  }

  /** Every scalar leaf value in a JSON document, recursively. */
  private def leafValues(node: JsonNode): Seq[String] = {
    if (node.isObject) {
      node.fields().asScala.toSeq.flatMap(e => leafValues(e.getValue))
    } else if (node.isArray) {
      node.elements().asScala.toSeq.flatMap(leafValues)
    } else {
      Seq(node.asText())
    }
  }

  private def assertNoCanary(scope: String, text: String, canaries: Seq[String]): Unit =
    canaries.foreach { canary =>
      withClue(s"[$scope] leaked canary [$canary] in: $text\n") {
        text should not include canary
      }
    }

  /** Substring scan of the whole line plus a structural scan of every leaf. */
  private def assertJsonHasNoCanary(scope: String, json: String, canaries: Seq[String]): Unit = {
    assertNoCanary(s"$scope raw", json, canaries)
    val leaves = leafValues(jsonMapper.readTree(json))
    canaries.foreach { canary =>
      withClue(s"[$scope leaf] leaked canary [$canary] in $leaves\n") {
        leaves.exists(_.contains(canary)) shouldBe false
      }
    }
  }

  // A sanitized Spark-native message must be exactly the bracketed errorClass
  // token (optionally followed by sqlState) that downstream consumers key on.
  private val bracketedErrorClass = "^\\[[A-Z0-9_.]+\\]( sqlState=\\[[^\\]]+\\])?$".r

  test("production JSON serializer emits no query content for a real analysis failure") {
    spark.sql("SELECT 1 AS id, 'x' AS name").createOrReplaceTempView("real_serializer_probe")
    val column = "nonexistent_col_serializer_9099"
    val root = failWith(s"SELECT $column FROM real_serializer_probe")
    val redacted = FlintREPL.redactThrowable(root)

    val logLine = productionLogLine(new OperationMessage(redacted.getMessage, 400), redacted)

    // The emitted line parses as JSON and no leaf value carries query content.
    assertJsonHasNoCanary(
      "serializer",
      logLine,
      Seq(column, "Project", "SubqueryAlias", "== SQL =="))

    // Positive contract: the OTEL envelope and stable tokens survive.
    val tree = jsonMapper.readTree(logLine)
    tree.at("/attributes/exception.type").asText() shouldBe
      "org.apache.spark.sql.exception.RedactedException"
    leafValues(tree).exists(_.contains("UNRESOLVED_COLUMN")) shouldBe true
  }

  private case class CanaryCase(
      name: String,
      sql: String,
      canaries: Seq[String],
      ansi: Boolean = false,
      sparkThrowable: Boolean = true,
      expectCanaryInRaw: Boolean = true,
      setup: () => Unit = () => ())

  private val longCanary = ("L" * 1500) + "_canary_long_9015"

  // Built from code points so the source stays ASCII (scalastyle forbids
  // non-ASCII), while the running query still carries real multibyte content.
  private val unicodeCanary =
    "canary_" + Seq(0x00fc, 0x00ef, 0x00f6, 0x00e9).map(_.toChar).mkString + "_9014"

  private def matrixCases: Seq[CanaryCase] = Seq(
    CanaryCase(
      "unresolved column",
      "SELECT canary_col_unrsv_9001 FROM real_matrix_vals",
      Seq("canary_col_unrsv_9001")),
    CanaryCase(
      "missing table or view",
      "SELECT * FROM canary_tbl_missing_9002",
      Seq("canary_tbl_missing_9002")),
    CanaryCase(
      "ambiguous reference",
      "SELECT canary_ambig_9003 FROM amb_a JOIN amb_b",
      Seq("canary_ambig_9003"),
      setup = () => {
        spark.sql("SELECT 1 AS canary_ambig_9003").createOrReplaceTempView("amb_a")
        spark.sql("SELECT 2 AS canary_ambig_9003").createOrReplaceTempView("amb_b")
      }),
    CanaryCase(
      "unresolved routine",
      "SELECT canary_fn_9004(id) FROM real_matrix_vals",
      Seq("canary_fn_9004")),
    CanaryCase(
      "datatype mismatch",
      "SELECT sum(canary_dtm_9005) FROM (SELECT array(1, 2) AS canary_dtm_9005)",
      Seq("canary_dtm_9005")),
    CanaryCase(
      "parse error",
      "SELCT canary_parse_9006 FROM real_matrix_vals",
      Seq("canary_parse_9006")),
    CanaryCase(
      "divide by zero (ANSI)",
      "SELECT canary_div_col_9007 / 0 FROM (SELECT 5 AS canary_div_col_9007)",
      Seq("canary_div_col_9007"),
      ansi = true),
    CanaryCase(
      "invalid cast (ANSI)",
      "SELECT CAST('canary_cast_9008' AS INT)",
      Seq("canary_cast_9008"),
      ansi = true),
    CanaryCase(
      "array index out of bounds (ANSI)",
      "SELECT array(canary_arr_9009)[10] FROM (SELECT 7 AS canary_arr_9009)",
      Seq("canary_arr_9009"),
      ansi = true),
    CanaryCase(
      "invalid regex pattern (executor exception, unknown-policy path)",
      "SELECT canary_rgx_9010col RLIKE '[' FROM (SELECT 'x' AS canary_rgx_9010col)",
      Seq("canary_rgx_9010col"),
      sparkThrowable = false,
      expectCanaryInRaw = false),
    CanaryCase(
      "nested CTE + subquery + join plan",
      "WITH cte AS (SELECT id, name FROM real_matrix_vals) " +
        "SELECT c.id FROM cte c JOIN real_matrix_vals r ON c.id = r.id " +
        "WHERE c.canary_nested_9011 > 0",
      Seq("canary_nested_9011")),
    CanaryCase(
      "ARN-shaped backticked identifier",
      "SELECT `arn:aws:iam::123456789012:role/canary_arn_9012` FROM real_matrix_vals",
      Seq("arn:aws:iam::123456789012:role/canary_arn_9012", "canary_arn_9012")),
    CanaryCase(
      "CRLF literal (ANSI)",
      "SELECT CAST('canary_crlf_9013\r\nSECOND_LINE_9013' AS INT)",
      Seq("canary_crlf_9013", "SECOND_LINE_9013"),
      ansi = true),
    CanaryCase(
      "Unicode backticked identifier",
      s"SELECT `$unicodeCanary` FROM real_matrix_vals",
      Seq(unicodeCanary)),
    CanaryCase(
      "very long backticked identifier",
      s"SELECT `$longCanary` FROM real_matrix_vals",
      Seq("_canary_long_9015")))

  matrixCases.foreach { c =>
    test(s"canary matrix: ${c.name}") {
      spark.sql("SELECT 1 AS id, 'x' AS name").createOrReplaceTempView("real_matrix_vals")
      c.setup()

      val root =
        if (c.ansi) {
          spark.conf.set("spark.sql.ansi.enabled", "true")
          try failWith(c.sql)
          finally spark.conf.unset("spark.sql.ansi.enabled")
        } else {
          failWith(c.sql)
        }

      // Precondition: the raw failure really carries the canary, so redaction is
      // proven rather than trivially satisfied. The executor-regex case is a
      // negative control -- its query text never reaches the pattern exception --
      // so it opts out of this precondition.
      if (c.expectCanaryInRaw) {
        val raw = Option(root.getMessage).getOrElse("")
        withClue(s"raw message did not contain any canary: $raw\n") {
          c.canaries.exists(raw.contains) shouldBe true
        }
      }

      // (1) sanitized message carries no canary ...
      val sanitized = ErrorSanitizer.sanitizedMessage(root)
      assertNoCanary("sanitized", sanitized, c.canaries)

      // ... and for a Spark-native failure it is exactly the bracketed errorClass
      // token (plus optional sqlState) preserved for downstream classification.
      if (c.sparkThrowable) {
        withClue(s"sanitized message was not a bare errorClass token: $sanitized\n") {
          bracketedErrorClass.pattern.matcher(sanitized).matches() shouldBe true
        }
      }

      // (2) full persisted error JSON, scanned recursively.
      assertJsonHasNoCanary("persistedJson", persistedJson(root), c.canaries)

      // (3) exact production log line for the redacted throwable, scanned
      // recursively across body.message, exception.message/type, and attributes.
      val redacted = FlintREPL.redactThrowable(root)
      val logLine = productionLogLine(new OperationMessage(redacted.getMessage, 400), redacted)
      assertJsonHasNoCanary("productionLogLine", logLine, c.canaries)
    }
  }
}
