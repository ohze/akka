/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package org.scalatest

// work around https://github.com/lampepfl/dotty/issues/8582
// usage: You must `extends SuiteMixin` first then `with BeforeAndAfterAll_8582`
trait BeforeAndAfterAll_8582 extends Suite { this: SuiteMixin =>
  val invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected = false
  protected def beforeAll() = ()
  protected def afterAll() = ()
  abstract override def run(testName: Option[String], args: Args): Status = {
    val (runStatus, thrownException) =
      try {
        if (!args.runTestInNewInstance && (expectedTestCount(args.filter) > 0 || invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected))
          beforeAll()
        (super.run(testName, args), None)
      }
      catch {
        case e: Exception => (FailedStatus, Some(e))
      }

    try {
      val statusToReturn =
        if (!args.runTestInNewInstance && (expectedTestCount(args.filter) > 0 || invokeBeforeAllAndAfterAllEvenIfNoTestsAreExpected)) {
          // runStatus may not be completed, call afterAll only after it is completed
          runStatus withAfterEffect {
            try {
              afterAll()
            }
            catch {
              case laterException: Exception if !Suite.anExceptionThatShouldCauseAnAbort(laterException) && thrownException.isDefined =>
              // We will swallow the exception thrown from after if it is not test-aborting and exception was already thrown by before or test itself.
            }
          }
        }
        else runStatus
      thrownException match {
        case Some(e) => throw e
        case None =>
      }
      statusToReturn
    }
    catch {
      case laterException: Exception =>
        thrownException match { // If both before/run and after throw an exception, report the earlier exception
          case Some(earlierException) => throw earlierException
          case None => throw laterException
        }
    }
  }
}
