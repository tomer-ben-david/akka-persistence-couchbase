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
import scala.util.Try

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

  implicit val timeout = Timeout(30.seconds)

  implicit val patienceConfig = PatienceConfig(timeout = Span.convertDurationToSpan(30.seconds))

  "Couchbase replay" must {

    "replay empty store" in new Fixture {
      val replayCollector = replay()
      replayCollector.replayedEvents shouldBe empty
    }

    "replay all events" in new Fixture {

      whenReady(createEvents(10)) { values =>
        val replayCollector = replay()

        replayCollector.hasFailures shouldBe false
        replayCollector.replayedEvents should have size values.size.toLong
        replayCollector.replayCompletes should contain theSameElementsAs Seq(Some(values.size - 1))
      }
    }

    "resume replay events" in new Fixture {

      whenReady(createEvents(10)) { values =>
        val replayCollector = replay()
        replayCollector.replayCompletes.headOption.get
      }

      whenReady(createEvents(10)) { values =>
        val replayCollector = replay()
        replayCollector.replayedEvents should have size values.size.toLong
      }
    }

    "resume replay events from specified journal id" in new Fixture {

      val journalMessageIdOption = whenReady(createEvents(10)) { values =>
        val replayCollector = replay()
        replayCollector.replayCompletes.headOption.get
      }

      whenReady(createEvents(10)) { values =>
        val replayCollector = replay(journalMessageIdOption)
        replayCollector.replayedEvents should have size values.size.toLong
      }
    }

    def createEvents(count: Int): Future[Seq[Any]] = {
      val futures = 0 until count map { i =>
        val echoActor = system.actorOf(EchoActor.props(i.toString))
        echoActor ? Echo(i) collect {
          case response => response
        }
      }

      Future.sequence(futures)
    }
  }

  class ReplayCollector extends ReplayCallback {

    var replayedEvents: Vector[(Long, Try[PersistentRepr])] = Vector.empty

    var replayCompletes: Vector[Option[Long]] = Vector.empty

    var replayFailures: Vector[Throwable] = Vector.empty

    override def replay(journalMessageId: Long, persistentReprAttempt: Try[PersistentRepr]): Unit = {
      replayedEvents = replayedEvents :+ journalMessageId -> persistentReprAttempt
    }

    override def onReplayComplete(journalMessageIdOption: Option[Long]): Unit = {
      replayCompletes = replayCompletes :+ journalMessageIdOption
    }

    override def onReplayFailed(reason: Throwable): Unit = {
      replayFailures = replayFailures :+ reason
    }

    def hasFailures = {
      replayedEvents.exists(_._2.isFailure)
    }
  }

  trait Fixture {
    val replayExtension = CouchbaseReplayExtension(system)

    def replay(journalMessageIdOption: Option[Long] = None): ReplayCollector = {
      val replayCollector = new ReplayCollector
      val countDownLatch = new CountDownLatch(1)

      replayExtension.replay(new ReplayCallback {

        override def replay(journalMessageId: Long, persistentReprAttempt: Try[PersistentRepr]): Unit = {
          replayCollector.replay(journalMessageId, persistentReprAttempt)
        }

        override def onReplayComplete(journalMessageIdOption: Option[Long]): Unit = {
          replayCollector.onReplayComplete(journalMessageIdOption)
          countDownLatch.countDown()
        }

        override def onReplayFailed(reason: Throwable): Unit = {
          replayCollector.onReplayFailed(reason)
          countDownLatch.countDown()
        }
      }, journalMessageIdOption)

      countDownLatch.await(timeout.duration.toSeconds, TimeUnit.SECONDS)
      replayCollector
    }
  }
}
