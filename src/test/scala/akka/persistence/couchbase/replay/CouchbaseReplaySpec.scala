package akka.persistence.couchbase.replay

import java.util.concurrent.{CountDownLatch, TimeUnit}

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.persistence.couchbase.replay.EchoActor.{Echo, Echoed}
import akka.persistence.couchbase.support.CouchbasePluginSpec
import akka.persistence.{PersistentActor, PersistentRepr, PluginSpec}
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures._
import org.scalatest.time.Span

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class EchoActor(val persistenceId: String) extends PersistentActor {

  override def receiveCommand: Receive = {

    case Echo(value) =>
      persist(Echoed(value)) { event =>
        sender() ! event
      }
  }

  override def receiveRecover: Receive = {
    case Echoed(value) =>
  }
}

object EchoActor {

  def props(persistenceId: String): Props = Props(classOf[EchoActor], persistenceId)

  case class Echo(value: Any)

  case class Echoed(value: Any)

}

class CouchbaseReplaySpec
  extends PluginSpec(CouchbasePluginSpec.config)
    with CouchbasePluginSpec {

  implicit lazy val system: ActorSystem = ActorSystem("ReplaySpec", config)

  implicit val ec = system.dispatcher

  implicit val timeout = Timeout(10.seconds)

  implicit val patienceConfig = PatienceConfig(timeout = Span.convertDurationToSpan(20.seconds))


  "Couchbase replay" must {

    "replay empty store" in {
      val replay = CouchbaseReplayExtension(system)
      replay.replay(new ReplayCallback {
        override def replay(journalMessageId: Long, any: Try[PersistentRepr]): Unit = {
          fail("Should not replay for empty store")
        }
      })
    }

    "replay events" in {

      val valueFutures = 1 to 100 map { i =>
        val echoActor = system.actorOf(EchoActor.props(i.toString), i.toString)
        echoActor ? Echo(i) collect {
          case response => response
        }
      }

      val latch = new CountDownLatch(1)

      whenReady(Future.sequence(valueFutures)) { values =>

        val replay = CouchbaseReplayExtension(system)

        replay.replay(new ReplayCallback {
          override def replay(journalMessageId: Long, persistentReprAttempt: Try[PersistentRepr]): Unit = persistentReprAttempt match {
            case Success(persistentRepr) =>
              values should contain (persistentRepr.payload)

            case Failure(e) =>
              fail(e)
          }

          override def onReplayComplete(journalMessageId: Long): Unit = {
            journalMessageId shouldBe valueFutures.size - 1
            latch.countDown()
          }
        })

        latch.await(timeout.duration.toSeconds, TimeUnit.SECONDS)
      }
    }
  }
}
