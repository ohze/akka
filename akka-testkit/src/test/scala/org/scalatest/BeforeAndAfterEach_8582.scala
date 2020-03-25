/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package org.scalatest

trait BeforeAndAfterEach_8582 extends Suite { this: SuiteMixin =>
  protected def beforeEach() = ()
  protected def afterEach() = ()

  abstract protected override def runTest(testName: String, args: Args): Status = {

    var thrownException: Option[Throwable] = None

    val runTestStatus: Status =
      try {
        if (!args.runTestInNewInstance) beforeEach()
        super.runTest(testName, args)
      }
      catch {
        case e: Throwable if !Suite.anExceptionThatShouldCauseAnAbort(e) =>
          thrownException = Some(e)
          FailedStatus
      }
    // And if the exception should cause an abort, abort the afterEach too.
    try {
      val statusToReturn: Status =
        if (!args.runTestInNewInstance) {
          runTestStatus withAfterEffect {
            try {
              afterEach()
            }
            catch {
              case e: Throwable if !Suite.anExceptionThatShouldCauseAnAbort(e) && thrownException.isDefined =>
              // We will swallow an exception thrown from afterEach if it is not test-aborting
              // and an exception was already thrown by beforeEach or test itself.
            }
          } // Make sure that afterEach is called even if (beforeEach or runTest) completes abruptly.
        }
        else
          runTestStatus
      thrownException match {
        case Some(e) => throw e
        case None =>
      }
      statusToReturn
    }
    catch {
      case laterException: Exception =>
        thrownException match {
          // If both (beforeEach or runTest) and afterEach throw an exception, throw the
          // earlier exception and swallow the later exception. The reason we swallow
          // the later exception rather than printing it is that it may be noisy because
          // it is caused by the beforeEach failing in the first place. Our goal with
          // this approach is to minimize the chances that a finite non-memory resource
          // acquired in beforeEach is not cleaned up in afterEach.
          case Some(earlierException) => throw earlierException
          case None => throw laterException
        }
    }
  }
}
