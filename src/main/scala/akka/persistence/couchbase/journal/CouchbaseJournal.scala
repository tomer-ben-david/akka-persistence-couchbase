package akka.persistence.couchbase.journal

import akka.actor.ActorLogging
import akka.persistence._
import akka.persistence.couchbase.{CouchbaseExtension, Message}
import akka.persistence.journal.{AsyncWriteJournal, Tagged}
import akka.serialization.SerializationExtension

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.util.{Success, Try}

class CouchbaseJournal extends AsyncWriteJournal with CouchbaseRecovery with CouchbaseStatements with ActorLogging {

  implicit val executionContext = context.dispatcher

  val couchbase = CouchbaseExtension(context.system)
  val serialization = SerializationExtension(context.system)

  def config = couchbase.journalConfig

  def bucket = couchbase.journalBucket

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {

    val serialized = messages.map(atomicWrite => Try {
      val persistenceId = atomicWrite.persistenceId
      atomicWrite.payload.map { persistentRepr =>

        val (persistent, tags) = persistentRepr.payload match {

          case Tagged(_payload, _tags) =>
            (persistentRepr.withPayload(_payload), _tags)

          case _ =>
            (persistentRepr, Set.empty[String])
        }


        val message = Message(serialization.findSerializerFor(persistent).toBinary(persistent))
        JournalMessage(persistenceId, persistent.sequenceNr, Marker.Message, Some(message), tags)
      }
    })

    val result = serialized.map(a => a.map(_ => ()))
    val batchResults = serialized.collect({ case Success(batch) => batch }).map(executeBatch)

    Future.sequence(batchResults).map(_ => result)
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {

    var toDelete: List[Long] = Nil

    CouchbaseRecovery.replayMessages(persistenceId, 0L, toSequenceNr, Long.MaxValue) { persistent =>
      if (!toDelete.headOption.contains(persistent.sequenceNr)) {
        toDelete = persistent.sequenceNr :: toDelete
      }
    }

    val asyncDeletions = toDelete.reverse.grouped(config.maxMessageBatchSize).map { group =>
      executeBatch {
        group.map(sequenceNr => JournalMessage(persistenceId, sequenceNr, Marker.MessageDeleted))
      }
    }

    Future.sequence(asyncDeletions).map(_ => ())
  }

  /**
    * Create design docs.
    */
  override def preStart(): Unit = {
    super.preStart()
    initDesignDocs()
  }
}
