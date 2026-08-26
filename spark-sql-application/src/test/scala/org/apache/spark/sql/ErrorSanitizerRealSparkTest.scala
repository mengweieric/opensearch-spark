/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import org.opensearch.flint.common.model.FlintStatement
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

  test("real unresolved-column AnalysisException: plan is stripped, diagnostic and class kept") {
    spark.sql("SELECT 1 AS id, 'x' AS name").createOrReplaceTempView("real_probe_vals")
    val column = "nonexistent_col_realspark_5551"
    val analysis = failWith(s"SELECT $column FROM real_probe_vals")

    // Precondition: the real message appends the resolved logical plan tree.
    analysis.getMessage should include("Project")
    analysis.isInstanceOf[AnalysisException] shouldBe true

    val sanitized = ErrorSanitizer.sanitizedMessage(analysis)
    // The plan tree and its node names are gone ...
    sanitized should not include "Project"
    sanitized should not include "SubqueryAlias"
    sanitized should not include "LocalRelation"
    sanitized should not include "View"
    // ... while the actionable diagnostic (error class and offending column) is retained. This is
    // the deliberate getSimpleMessage policy: the unresolved identifier stays for debuggability.
    sanitized should include("UNRESOLVED_COLUMN")
    sanitized should include(column)

    ErrorSanitizer.classify(analysis).errorCode shouldBe ErrorCode.QueryAnalysisError

    val json = persistedJson(analysis)
    json should not include "Project"
    json should include(""""errorCode":"QUERY_ANALYSIS_ERROR"""")
    json should include(
      """"exception.type":"org.apache.spark.sql.catalyst.ExtendedAnalysisException"""")
  }

  test("real ParseException: the == SQL == block and full query text are stripped") {
    val queryCanary = "bogus_realspark_6662"
    val parse = failWith(s"SELCT $queryCanary FROM real_probe_vals")

    parse.getMessage should include("== SQL ==")
    parse.getMessage should include(queryCanary)

    val sanitized = ErrorSanitizer.sanitizedMessage(parse)
    sanitized should not include "== SQL =="
    sanitized should not include queryCanary
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
}
