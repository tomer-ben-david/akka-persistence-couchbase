package akka.persistence.couchbase.journal

import java.util.UUID

import akka.actor.{Terminated, ActorSystem, ExtendedActorSystem, Props}
import akka.persistence.PersistentActor
import akka.persistence.couchbase.support.CouchbasePluginSpec
import akka.persistence.journal.{Tagged, WriteEventAdapter}
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest.{Matchers, WordSpecLike}

class Persistent(override val persistenceId: String) extends PersistentActor {

  var all: List[String] = Nil

  override def receiveCommand: Receive = {
    case text: String => persist(Event(text)) { event =>
      all = event.text :: all
    }

    case GetEvents =>
      sender() ! Events(all.reverse)

    case Terminate =>
      context.stop(self)
  }

  override def receiveRecover: Receive = {
    case Event(text) =>
      all = text :: all
  }
}

class TaggingEventAdapter(system: ExtendedActorSystem) extends WriteEventAdapter {

  override def manifest(event: Any): String = ""

  override def toJournal(event: Any): Any = event match {
    case Event(text) =>
      if (text.isEmpty) {
        event
      } else {
        Tagged(event, Set(text.substring(0, 1)))
      }
  }
}

case class Event(text: String)

case object GetEvents

case class Events(events: List[String])

case object Terminate

class CouchbaseTaggingJournalSpec
  extends TestKit(ActorSystem(
    UUID.randomUUID().toString,
    CouchbasePluginSpec.config.withFallback(ConfigFactory.parseString(
      """
        |couchbase-journal {
        |  event-adapters {
        |    tagging = "akka.persistence.couchbase.journal.TaggingEventAdapter"
        |  }
        |  event-adapter-bindings {
        |    "akka.persistence.couchbase.journal.Event" = tagging
        |  }
        |}
      """.stripMargin))
  ))
  with ImplicitSender
  with CouchbasePluginSpec
  with WordSpecLike
  with Matchers {

  "A journal" must {

    "save event tags" in {
      val actor = system.actorOf(Props(new Persistent("p")))

      // Save events with tags
      actor ! ""
      actor ! "a"
      actor ! "aa"
      actor ! "b"
      actor ! "bb"
      actor ! "c"
      actor ! "cc"

      actor ! GetEvents

      val events = expectMsgType[Events]
      events.events should have size 7

      // Stop
      watch(actor)
      actor ! Terminate
      expectMsgType[Terminated]

      // Recover
      val recovered = system.actorOf(Props(new Persistent("p")))
      recovered ! GetEvents

      val recoveredEvents = expectMsgType[Events]
      recoveredEvents.events should have size 7
    }
  }
}

