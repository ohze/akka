/*
 * Copyright (C) 2009-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.actor.testkit.typed.javadsl

import java.time.Duration

import akka.actor.typed.ActorSystem
import akka.actor.typed.internal.adapter.SchedulerAdapter
import com.typesafe.config.Config
import akka.util.JavaDurationConverters._

//import scala.annotation.varargs

/**
 * Manual time allows you to do async tests while controlling the scheduler of the system.
 *
 * To use it you need to configure the `ActorSystem`/`ActorTestKit` with [[ManualTime.config]] and access the
 * scheduler control through [[ManualTime.get]]
 */
object ManualTime {

  /**
   * Config that needs to be in place for the actor system to use the manual
   */
  def config(): Config = akka.actor.testkit.typed.scaladsl.ManualTime.config

  /**
   * Access the manual scheduler, note that you need to setup the actor system/testkit with [[ManualTime.config]]
   * for this to work.
   */
  def get[A](system: ActorSystem[A]): ManualTime =
    system.scheduler match {
      case adapter: SchedulerAdapter =>
        adapter.classicScheduler match {
          case sc: akka.testkit.ExplicitlyTriggeredScheduler => new ManualTime(sc)
          case _ =>
            throw new IllegalArgumentException(
              "ActorSystem not configured with explicitly triggered scheduler, " +
              "make sure to include akka.actor.testkit.typed.scaladsl.ManualTime.config() when setting up the test")
        }
      case s =>
        throw new IllegalArgumentException(
          s"ActorSystem.scheduler is not a classic SchedulerAdapter but a ${s.getClass.getName}, this is not supported")
    }

}

/**
 * Not for user instantiation, see [[ManualTime#get]]
 */
final class ManualTime(delegate: akka.testkit.ExplicitlyTriggeredScheduler) {

  /**
   * Advance the clock by the specified duration, executing all outstanding jobs on the calling thread before returning.
   *
   * We will not add a dilation factor to this amount, since the scheduler API also does not apply dilation.
   * If you want the amount of time passed to be dilated, apply the dilation before passing the delay to
   * this method.
   */
  def timePasses(amount: Duration): Unit = delegate.timePasses(amount.asScala)

//  @varargs
// https://github.com/lampepfl/dotty/issues/7212
  import scala.collection.immutable
  def expectNoMessageFor(duration: Duration, on: immutable.Seq[TestProbe[_]]): Unit = {
    delegate.timePasses(duration.asScala)
    on.foreach(_.expectNoMessage(Duration.ZERO))
  }
  // https://github.com/lampepfl/dotty/issues/7212
  def expectNoMessageFor(duration: Duration): Unit =
    expectNoMessageFor(duration, immutable.Seq())
  def expectNoMessageFor(duration: Duration, a1: TestProbe[_]): Unit =
    expectNoMessageFor(duration, immutable.Seq(a1))
  def expectNoMessageFor(duration: Duration, a1: TestProbe[_], a2: TestProbe[_]): Unit =
    expectNoMessageFor(duration, immutable.Seq(a1, a2))
  def expectNoMessageFor(duration: Duration, a1: TestProbe[_], a2: TestProbe[_], a3: TestProbe[_]): Unit =
    expectNoMessageFor(duration, immutable.Seq(a1, a2, a3))
}
