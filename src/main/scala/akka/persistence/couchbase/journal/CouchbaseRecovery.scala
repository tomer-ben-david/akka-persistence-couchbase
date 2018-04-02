package akka.persistence.couchbase.journal

import java.util.concurrent.TimeUnit

import akka.persistence.PersistentRepr
import akka.persistence.couchbase.{CouchbaseExtension, LogUtils}
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.error.ViewDoesNotExistException

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Try

trait CouchbaseRecovery {
  this: CouchbaseJournal =>

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] = {
    Future.fromTry(CouchbaseRecovery.replayMessages(persistenceId, fromSequenceNr, toSequenceNr, max)(replayCallback))
  }

  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    Future.fromTry(CouchbaseRecovery.readHighestSequenceNr(persistenceId, fromSequenceNr)).recoverWith {
      case view: ViewDoesNotExistException =>
        log.error(s"asyncReadHighestSequenceNr view does not exist $view, recreating...")
        CouchbaseExtension.recreateViews()
        Future.fromTry(CouchbaseRecovery.readHighestSequenceNr(persistenceId, fromSequenceNr))
    }
  }

  object CouchbaseRecovery {

    def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Try[Long] = {
      Try {
        val iterator = new PersistentIterator(persistenceId, math.max(1L, fromSequenceNr), Long.MaxValue, Long.MaxValue)
        while (iterator.hasNext) iterator.next()
        iterator.highestSequenceNr
      }
    }

    def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Try[Unit] = {
      Try(new PersistentIterator(persistenceId, fromSequenceNr, toSequenceNr, max).foreach(replayCallback))
    }

    def retrieveMessages(max: Long, fromSequenceNr: Long, toSequenceNr: Long, persistenceId: String): Iterator[JournalMessage] ={
      if (max == 0 || fromSequenceNr > toSequenceNr) {
        List.empty[JournalMessage].iterator
      } else {
        val query = bySequenceNr(persistenceId, fromSequenceNr, toSequenceNr)
        log.info(s"${LogUtils.CBPersistenceKey}.JOURNAL Before query timeout ${config.timeout.toSeconds}")
        val result = bucket.query(query, config.timeout.toSeconds, TimeUnit.SECONDS).iterator.asScala.map { viewRow =>
          JournalMessage.deserialize(viewRow.value().asInstanceOf[JsonObject], viewRow.id())
        }
        log.info(s"${LogUtils.CBPersistenceKey}.JOURNAL After query timeout ${config.timeout.toSeconds}")
        result
      }
    }

    /**
      * Iterator over persistent repr.
      */
    class PersistentIterator(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long) extends Iterator[PersistentRepr] {

      private var highestSequenceNrInternal = 0L

      def highestSequenceNr = highestSequenceNrInternal

      private val messageIterator = retrieveMessages(max, fromSequenceNr, toSequenceNr, persistenceId)
      private var remaining = max
      private var current: List[PersistentRepr] = List.empty
      private var upcoming: List[PersistentRepr] = List.empty

      fetch()

      def hasNext: Boolean = {
        (current.nonEmpty || upcoming.nonEmpty) && remaining > 0
      }

      def next(): PersistentRepr = {

        if (!hasNext) {
          throw new NoSuchElementException
        }

        remaining = remaining - 1

        if (current.isEmpty) {
          fetch()
        }

        val result = current.headOption.getOrElse(throw new NoSuchElementException)
        current = current.tail
        result
      }

      private def fetch(): Unit = {
        current = upcoming
        upcoming = List.empty

        var deleted: Option[Long] = None

        while (messageIterator.hasNext && upcoming.isEmpty) {

          val journalMessage = messageIterator.next()

          highestSequenceNrInternal = journalMessage.sequenceNr

          journalMessage.marker match {
            case Marker.Message =>

              if (!deleted.contains(journalMessage.sequenceNr)) {

                journalMessage.message.foreach { message =>
                  val serializer = serialization.serializerFor(classOf[PersistentRepr])
                  val persistent = serializer.fromBinary(message.bytes).asInstanceOf[PersistentRepr]

                  if (current.headOption.exists(_.sequenceNr != journalMessage.sequenceNr)) {
                    upcoming = persistent :: Nil
                  } else {
                    current = persistent :: current
                  }
                }
              }

            case Marker.MessageDeleted =>
              deleted = Some(journalMessage.sequenceNr)

              if (current.headOption.exists(_.sequenceNr == journalMessage.sequenceNr)) {
                current = List.empty
              }
          }
        }

        current = current.reverse
      }
    }

  }

}
