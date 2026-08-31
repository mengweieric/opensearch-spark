/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.util

import org.opensearch.flint.core.logging.CustomLogging

import org.apache.spark.sql.ErrorSanitizer

/**
 * Handles and manages exceptions and error messages during each emr job run. Provides methods to
 * set, retrieve, and reset exception information.
 */
class ThrowableHandler {
  private var _throwableOption: Option[Throwable] = None
  private var _error: String = _

  def exceptionThrown: Option[Throwable] = _throwableOption
  def error: String = _error

  def recordThrowable(err: String, t: Throwable): Unit = {
    _error = err
    _throwableOption = Some(t)
    CustomLogging.logError(err, t)
  }

  /**
   * Records a throwable that arises on a query-execution path (statement execution, query
   * preparation, result writing, statement or session update, or an escaped query / session
   * loop). Unlike [[recordThrowable]], which is reserved for infrastructure failures whose
   * message and throwable cannot carry query content (heartbeat, session-store reads/writes,
   * Spark session shutdown), this method never lets the raw throwable, its rendered message, an
   * interpolated customer value, or the persisted customer error JSON reach the broadly-readable
   * driver log.
   *
   * The `persistedError` string is stored for the customer-facing / forwarded record exactly as
   * the caller built it, so the persisted diagnostic is unchanged. The driver log instead
   * receives only the caller's static `operatorContext` label (which must contain no query text,
   * identifier, literal, or customer error JSON) followed by the strict
   * [[ErrorSanitizer.operatorLogMessage]] for `t`, and a redacted throwable whose message and
   * stack-trace header expose only that strict label while preserving the original type name and
   * frames. Classification consumers still observe the original throwable through
   * [[exceptionThrown]].
   */
  def recordQueryThrowable(
      persistedError: String,
      operatorContext: String,
      t: Throwable): Unit = {
    _error = persistedError
    _throwableOption = Some(t)
    val (logMessage, logSafeThrowable) = operatorLogRendering(operatorContext, t)
    CustomLogging.logError(logMessage, logSafeThrowable)
  }

  /**
   * The sanitized (message, throwable) pair written to the driver log by
   * [[recordQueryThrowable]]. Exposed for tests so the no-leak contract can be asserted directly
   * on the exact values handed to the logger, without capturing log4j output.
   */
  private[sql] def operatorLogRendering(
      operatorContext: String,
      t: Throwable): (String, Throwable) =
    (
      s"$operatorContext: ${ErrorSanitizer.operatorLogMessage(t)}",
      ErrorSanitizer.redactThrowable(t))

  def setError(err: String): Unit = {
    _error = err
  }

  def setThrowable(t: Throwable): Unit = {
    _throwableOption = Some(t)
  }

  def reset(): Unit = {
    _throwableOption = None
    _error = null
  }

  def hasException: Boolean = _throwableOption.isDefined
}
