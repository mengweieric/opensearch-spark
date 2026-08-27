/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql

import com.amazonaws.services.glue.model.AWSGlueException
import com.amazonaws.services.s3.model.AmazonS3Exception
import org.opensearch.{OpenSearchException, OpenSearchSecurityException}
import org.opensearch.action.DocWriteRequest.OpType
import org.opensearch.action.bulk.{BulkItemResponse, BulkResponse}
import org.opensearch.action.bulk.BulkItemResponse.Failure
import org.opensearch.flint.core.storage.OpenSearchBulkWriteException
import org.opensearch.rest.RestStatus
import org.scalatest.matchers.should.Matchers

import org.apache.spark.{SparkException, SparkFunSuite, SparkThrowable}
import org.apache.spark.sql.ErrorSanitizer.ErrorCode
import org.apache.spark.sql.catalyst.ExtendedAnalysisException
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types.StringType

/**
 * Covers the two halves of the error contract separately: the sanitized message (what a human
 * reads) and the classification (what a consumer branches on). The point of the split is that the
 * second must not depend on the wording of the first.
 */
class ErrorSanitizerTest extends SparkFunSuite with Matchers {

  private def bulkWriteException(
      status: RestStatus,
      cause: Exception = new OpenSearchSecurityException("no permissions"))
      : OpenSearchBulkWriteException = {
    val item =
      new BulkItemResponse(0, OpType.INDEX, new Failure("myindex", "doc-1", cause, status))
    OpenSearchBulkWriteException.from("myindex", new BulkResponse(Array(item), 100L), _ => true)
  }

  // ---- classification ----

  test("classify maps a forbidden bulk write to access-denied with its status code") {
    val classification = ErrorSanitizer.classify(bulkWriteException(RestStatus.FORBIDDEN))

    classification.errorCode shouldBe ErrorCode.OpenSearchWriteAccessDenied
    classification.statusCode shouldBe Some(403)
  }

  test("classify keeps a non-security 403 as a general OpenSearch write error") {
    // OpenSearch 2.6 reconstructs server errors as generic OpenSearchException instances and keeps
    // the original server type in this structured prefix. A readonly index produces this 403 shape;
    // status alone must not turn it into a customer authorization failure.
    val clusterBlock = new OpenSearchException(
      "OpenSearch exception [type=cluster_block_exception, reason=index read-only]")
    val e = bulkWriteException(RestStatus.FORBIDDEN, clusterBlock)

    val classification = ErrorSanitizer.classify(e)
    val sanitized = ErrorSanitizer.sanitizedMessage(e)

    classification.errorCode shouldBe ErrorCode.OpenSearchWriteError
    classification.statusCode shouldBe Some(403)
    sanitized should include("type=cluster_block_exception")
    sanitized should not include "type=authorization_exception"
    sanitized should not include "reason=OpenSearch exception"
  }

  test("classify distinguishes a non-authorization bulk write failure from access denied") {
    val classification =
      ErrorSanitizer.classify(bulkWriteException(RestStatus.TOO_MANY_REQUESTS))

    classification.errorCode shouldBe ErrorCode.OpenSearchWriteError
    classification.statusCode shouldBe Some(429)
  }

  test("classify finds a typed failure wrapped inside a SparkException") {
    // Real shape: a write failure surfaces to the driver wrapped in a task-failure SparkException.
    val wrapped =
      new SparkException(
        "Job aborted due to stage failure",
        bulkWriteException(RestStatus.FORBIDDEN))

    val classification = ErrorSanitizer.classify(wrapped)

    classification.errorCode shouldBe ErrorCode.OpenSearchWriteAccessDenied
    classification.statusCode shouldBe Some(403)
  }

  test("classify reads the status from a typed AWS exception rather than its message") {
    val s3 = new AmazonS3Exception("Access Denied")
    s3.setStatusCode(403)
    s3.setServiceName("Amazon S3")

    val classification = ErrorSanitizer.classify(s3)

    classification.errorCode shouldBe ErrorCode.S3Error
    classification.statusCode shouldBe Some(403)
  }

  test(
    "classify maps a Glue AccessDeniedException to access-denied even when the SDK status is 400") {
    // Glue reports authorization failures with the concrete AccessDeniedException type but an HTTP
    // status of 400, so the status alone would misclassify it as a generic Glue error.
    val glue = new com.amazonaws.services.glue.model.AccessDeniedException("denied")
    glue.setStatusCode(400)
    glue.setServiceName("AWSGlue")

    val classification = ErrorSanitizer.classify(glue)

    classification.errorCode shouldBe ErrorCode.GlueAccessDenied
    classification.statusCode shouldBe Some(400)
  }

  test("classify maps a SparkThrowable that is not a query exception to a spark query error") {
    val e = new RuntimeException("boom") with SparkThrowable {
      override def getErrorClass: String = "SOME_SPARK_ERROR"
    }

    ErrorSanitizer.classify(e).errorCode shouldBe ErrorCode.SparkQueryError
  }

  test("classify treats an authorization failure in a mixed bulk write as access denied") {
    val forbidden = new BulkItemResponse(
      0,
      OpType.INDEX,
      new Failure(
        "myindex",
        "doc-1",
        new OpenSearchSecurityException("denied"),
        RestStatus.FORBIDDEN))
    val throttled = new BulkItemResponse(
      1,
      OpType.INDEX,
      new Failure("myindex", "doc-2", new RuntimeException("busy"), RestStatus.TOO_MANY_REQUESTS))
    // The throttled item is listed first to prove 403 wins irrespective of item order.
    val e = OpenSearchBulkWriteException.from(
      "myindex",
      new BulkResponse(Array(throttled, forbidden), 100L),
      _ => true)

    val classification = ErrorSanitizer.classify(e)
    classification.errorCode shouldBe ErrorCode.OpenSearchWriteAccessDenied
    classification.statusCode shouldBe Some(403)
  }

  test("classify separates a syntax error from a general analysis error") {
    ErrorSanitizer
      .classify(
        new ExtendedAnalysisException(
          message = "[UNRESOLVED_COLUMN] cannot resolve column",
          line = Some(1),
          startPosition = Some(1),
          plan = Some(LocalRelation(AttributeReference("c", StringType)()))))
      .errorCode shouldBe ErrorCode.QueryAnalysisError
  }

  test("classify falls back to unknown without a status for an unrecognized throwable") {
    val classification = ErrorSanitizer.classify(new IllegalStateException("something broke"))

    classification.errorCode shouldBe ErrorCode.Unknown
    classification.statusCode shouldBe None
  }

  test("classify tolerates a self-referencing cause chain") {
    // A throwable whose cause is itself would loop a naive walker.
    val selfReferencing = new RuntimeException("loop") {
      override def getCause: Throwable = this
    }

    ErrorSanitizer.classify(selfReferencing).errorCode shouldBe ErrorCode.Unknown
  }

  // ---- sanitized message ----

  test("sanitizedMessage keeps the bulk write classification but drops per-item failure text") {
    val documentValue = "customer-secret-field-value"
    val e = bulkWriteException(
      RestStatus.FORBIDDEN,
      new OpenSearchSecurityException(s"no permissions for $documentValue"))

    val sanitized = ErrorSanitizer.sanitizedMessage(e)

    sanitized should include("statusCode=[403]")
    sanitized should include(
      "type=security_exception, reason=OpenSearch exception [type=authorization_exception]")
    sanitized should not include documentValue
    sanitized should not include "myindex"
  }

  test("sanitizedMessage returns structured fields for an AWS exception, not its raw message") {
    val s3 = new AmazonS3Exception("Access Denied for s3://customer-bucket/secret-prefix/file")
    s3.setStatusCode(403)
    s3.setServiceName("Amazon S3")
    s3.setErrorCode("AccessDenied")

    val sanitized = ErrorSanitizer.sanitizedMessage(s3)

    sanitized should include("statusCode=[403]")
    sanitized should include("errorCode=[AccessDenied]")
    // The raw message can embed customer bucket and key names, so it must not be carried over.
    sanitized should not include "customer-bucket"
    sanitized should not include "secret-prefix"
  }

  test(
    "sanitizedMessage keeps a non-S3 AWS message so a deliberately curated one is not discarded") {
    // processQueryException replaces a Glue access-denied message with an actionable explanation.
    // Reducing every AWS exception to an error code would throw that away, so other AWS families
    // keep their first line.
    val glue =
      new AWSGlueException("Access denied in AWS Glue service. Please check permissions.")
    glue.setStatusCode(400)
    glue.setServiceName("AWSGlue")

    ErrorSanitizer.sanitizedMessage(glue) should include(
      "Access denied in AWS Glue service. Please check permissions.")
  }

  test(
    "sanitizedMessage for a real Spark 3.5.1 error-class exception exposes only the errorClass and " +
      "sqlState, never the message parameters") {
    // A real Spark 3.5.1 SparkThrowable built through the error-class constructor, so getMessage is
    // rendered by SparkThrowableHelper from error-classes.json with the parameter interpolated --
    // exactly the path that leaks customer identifiers. COLUMN_ALREADY_EXISTS takes a single
    // `columnName` parameter and carries sqlState 42711 in the catalog.
    val secret = "customer_secret_column_CANARY_ACCOUNT_A"
    val e = new SparkException(
      "COLUMN_ALREADY_EXISTS",
      Map("columnName" -> secret),
      /* cause = */ null)

    // Precondition: the rendered message really does interpolate the secret parameter value.
    e.getMessage should include(secret)

    val sanitized = ErrorSanitizer.sanitizedMessage(e)

    // Only the stable, catalog-derived identifiers survive; the parameter value does not.
    sanitized shouldBe "[COLUMN_ALREADY_EXISTS] sqlState=[42711]"
    sanitized should not include secret
  }

  test("sanitizedMessage drops a SparkThrowable's rendered message and query context entirely") {
    // Even when the throwable carries a multi-line message with an "== SQL ==" query fragment, none
    // of it is read: the policy emits the errorClass only (no sqlState overridden here).
    val e = new RuntimeException(
      "[DIVIDE_BY_ZERO] Division by zero.\n== SQL ==\nSELECT secret_value / 0 FROM customer_table")
      with SparkThrowable {
      override def getErrorClass: String = "DIVIDE_BY_ZERO"
    }

    val sanitized = ErrorSanitizer.sanitizedMessage(e)

    // The errorClass leads; the rendered message and its query fragment are entirely gone. The
    // SparkThrowable default derives sqlState from the error-conditions catalog, so only the
    // errorClass prefix is pinned exactly.
    sanitized should startWith("[DIVIDE_BY_ZERO]")
    sanitized should not include "== SQL =="
    sanitized should not include "secret_value"
    sanitized should not include "customer_table"
    sanitized should not include "Division by zero"
  }

  test("sanitizedMessage appends sqlState for a SparkThrowable that reports one") {
    val e = new RuntimeException("ignored rendered message") with SparkThrowable {
      override def getErrorClass: String = "SOME_ERROR_CLASS"
      override def getSqlState: String = "42P01"
    }

    ErrorSanitizer.sanitizedMessage(e) shouldBe "[SOME_ERROR_CLASS] sqlState=[42P01]"
  }

  test("sanitizedMessage falls back to a bare label for a SparkThrowable with no errorClass") {
    val e = new RuntimeException("rendered message that must not surface") with SparkThrowable {
      override def getErrorClass: String = null
    }

    ErrorSanitizer.sanitizedMessage(e) shouldBe "[SPARK_ERROR]"
  }

  test(
    "sanitizedMessage reduces a hand-built analysis exception to a bare label, recovering no " +
      "token from its free-text message") {
    val planMarker = "CANARY_ACCOUNT_A"
    val plan = LocalRelation(AttributeReference(s"recipientAccountId_$planMarker", StringType)())
    // A hand-built ExtendedAnalysisException carries no catalog errorClass, so getErrorClass is
    // null. The strict policy must fall back to a bare label rather than parsing tokens (e.g. the
    // bracketed "[UNRESOLVED_COLUMN]") out of the free-text message or reading getSimpleMessage.
    val e = new ExtendedAnalysisException(
      message = "[UNRESOLVED_COLUMN] cannot resolve `arn`",
      line = Some(1),
      startPosition = Some(295),
      plan = Some(plan))

    // Precondition: the raw message really does leak the plan and query-derived identifiers.
    e.getMessage should include(planMarker)
    e.getMessage should include("arn")

    val sanitized = ErrorSanitizer.sanitizedMessage(e)

    sanitized shouldBe "[SPARK_ERROR]"
    // Nothing derived from the free-text message survives: not the bracketed pseudo-errorClass,
    // not the offending identifier, not the plan, not the line/position text.
    sanitized should not include "UNRESOLVED_COLUMN"
    sanitized should not include "arn"
    sanitized should not include planMarker
    sanitized should not include "LocalRelation"
    sanitized should not include "pos 295"
  }

  test("sanitizedMessage fails closed when policy evaluation itself throws") {
    // If the sanitizer cannot complete, we cannot assert anything about what the original message
    // contains, so returning or logging it would be fail-open. The fallback carries no detail.
    val secret = "customer-secret-literal"
    val hostile = new RuntimeException(secret) {
      override def getMessage: String = throw new IllegalStateException("cannot render")
    }

    val sanitized = ErrorSanitizer.sanitizedMessage(hostile)

    sanitized shouldBe ErrorSanitizer.RedactedFallbackMessage
    sanitized should not include secret
  }

  test("sanitizedMessage returns an empty string for a null message without throwing") {
    ErrorSanitizer.sanitizedMessage(new RuntimeException()) shouldBe ""
    ErrorSanitizer.sanitizedMessage(new NullPointerException()) shouldBe ""
  }

  test("sanitizedMessage leaves a single-line unrecognized message untouched") {
    ErrorSanitizer.sanitizedMessage(
      new IllegalArgumentException("bad arg value 42")) shouldBe "bad arg value 42"
  }

  test("sanitizedMessage keeps only the first line of an unrecognized multiline message") {
    ErrorSanitizer.sanitizedMessage(
      new RuntimeException("summary line\ndetail-line\nmore-detail")) shouldBe "summary line"
  }
}
