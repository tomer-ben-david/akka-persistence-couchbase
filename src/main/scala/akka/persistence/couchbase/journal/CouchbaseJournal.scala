package akka.persistence.couchbase.journal

import akka.actor.ActorLogging
import akka.persistence._
import akka.persistence.couchbase.{Couchbase, CouchbaseExtension, LogUtils, Message}
import akka.persistence.journal.{AsyncWriteJournal, Tagged}
import akka.serialization.{SerializationExtension, SerializerWithStringManifest}

import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.util.{Success, Try}

class CouchbaseJournal extends AsyncWriteJournal with CouchbaseRecovery with CouchbaseStatements with ActorLogging {

  implicit val executionContext = context.dispatcher

  val couchbase = CouchbaseExtension(context.system)
  val serialization = SerializationExtension(context.system)

  override def config = couchbase.journalConfig

  override def bucket = couchbase.journalBucket

  val tombstone = couchbase.journalConfig.tombstone

  override def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {

    val funcName = "asyncWriteMessages"

    log.info(s"${LogUtils.CBPersistenceKey}.$funcName: about to persist $messages")

    val serialized = messages.map(atomicWrite => Try {
      val persistenceId = atomicWrite.persistenceId
      atomicWrite.payload.map { persistentRepr =>

        val (persistent, tags) = persistentRepr.payload match {

          case Tagged(_payload, _tags) =>
            (persistentRepr.withPayload(_payload), _tags)

          case _ =>
            (persistentRepr, Set.empty[String])
        }

        val event = persistent.payload.asInstanceOf[AnyRef]
        val serializer = serialization.findSerializerFor(event)
        val manifestOption = serializer match {
          case serializerWithStringManifest: SerializerWithStringManifest =>
            Some(serializerWithStringManifest.manifest(event))
          case _ =>
            if (serializer.includeManifest) {
              Some(event.getClass.getName)
            }
            else {
              None
            }
        }

        val message = Message(serialization.findSerializerFor(persistent).toBinary(persistent))
        JournalMessage(persistenceId, persistent.sequenceNr, Marker.Message, manifestOption, Some(message), tags)
      }
    })

    val result = serialized.map(a => a.map(_ => ()))
    val batches = serialized.collect({ case Success(batch) => batch })

    val finalResult = Future.fromTry {
      batches.foldLeft[Try[Unit]](Success({})) { case (acc, batch) =>
        acc.flatMap { _ =>
          log.info(s"${LogUtils.CBPersistenceKey}.$funcName: JOURNAL Before execute batch")
          val result = executeBatch(batch)
          log.info(s"${LogUtils.CBPersistenceKey}.$funcName: JOURNAL after execute batch")
          result
        }
      }.map(_ => result)
    }

    log.info(s"${LogUtils.CBPersistenceKey}.$funcName: finished persisting $messages result: $finalResult")
    finalResult.recoverWith {
      case t: Throwable =>
        log.error(s"${LogUtils.CBPersistenceKey}.$funcName: error when persisting $messages result: $finalResult, exception: $t")
        log.info(s"${LogUtils.CBPersistenceKey}.$funcName: reconnect after error persisting")
        CouchbaseExtension.reconnectJournalBucket()
        throw t
    }
    finalResult
  }

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] = {
    Future.fromTry[Unit] {
      if (tombstone)
        // add tombstone document
        addTombstone(persistenceId, toSequenceNr)
      else
        //physically remove journals from the bucket
        physicalRemove(persistenceId, toSequenceNr)
    }
  }

  def physicalRemove(persistenceId: String, toSequenceNr: Long): Try[Unit] = {
    var toDelete: List[String] = Nil

    val journalMessage = CouchbaseRecovery.retrieveMessages(Long.MaxValue, 0L, toSequenceNr, persistenceId)

    journalMessage.foreach(journalMessage => {
      journalMessage.journalId match {
          case Some(journalId) => toDelete = journalId :: toDelete
          case None => "do nothing"
      }
    })

    if (toDelete.nonEmpty) //if the list contains any ids then remove them from the bucket
      deleteBatch(toDelete)
    else { //there is nothing to remove from the bucket so just log and exit
      log.debug("There is nothing to remove")
      Try{}
    }
  }

  def addTombstone(persistenceId: String, toSequenceNr: Long): Try[Unit] = {
    var toDelete: List[Long] = Nil
    CouchbaseRecovery.replayMessages(persistenceId, 0L, toSequenceNr, Long.MaxValue) { persistent =>
      if (!toDelete.headOption.contains(persistent.sequenceNr)) {
        toDelete = persistent.sequenceNr :: toDelete
      }
    }.flatMap { _ =>
      val groups = toDelete.reverse.grouped(config.maxMessageBatchSize)
      groups.foldLeft[Try[Unit]](Success({})) { case (acc, group) =>
        acc.flatMap { _ =>
          executeBatch {
            group.map(sequenceNr => JournalMessage(persistenceId, sequenceNr, Marker.MessageDeleted))
          }
        }
      }
    }
  }

}
