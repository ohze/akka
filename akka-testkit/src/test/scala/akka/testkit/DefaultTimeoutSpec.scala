/*
 * Copyright (C) 2013-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterAll_8582, SuiteMixin}
import akka.actor.ActorSystem
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.{AnyWordSpec, AnyWordSpecLike}

class DefaultTimeoutSpec extends SuiteMixin with AnyWordSpecLike with Matchers with BeforeAndAfterAll_8582 with TestKitBase with DefaultTimeout {

  implicit lazy val system = ActorSystem("AkkaCustomSpec")

  override def afterAll() = system.terminate()

  "A spec with DefaultTimeout" should {
    "use timeout from settings" in {
      timeout should ===(testKitSettings.DefaultTimeout)
    }
  }
}
