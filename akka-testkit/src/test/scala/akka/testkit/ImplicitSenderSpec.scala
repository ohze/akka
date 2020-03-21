/*
 * Copyright (C) 2013-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.testkit

import akka.actor.ActorSystem
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterAll_8582, SuiteMixin}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.{AnyWordSpec, AnyWordSpecLike}

class ImplicitSenderSpec extends SuiteMixin with AnyWordSpecLike with Matchers with BeforeAndAfterAll_8582 with TestKitBase with ImplicitSender {

  implicit lazy val system: ActorSystem = ActorSystem("AkkaCustomSpec")

  override def afterAll() = system.terminate()

  "An ImplicitSender" should {
    "have testActor as its self" in {
      self should ===(testActor)
    }
  }
}
