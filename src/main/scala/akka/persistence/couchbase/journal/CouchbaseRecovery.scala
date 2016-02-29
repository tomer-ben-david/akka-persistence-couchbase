package akka.persistence.couchbase.journal

import akka.persistence.PersistentRepr
import com.couchbase.client.java.document.json.JsonObject

import scala.collection.JavaConverters._
import scala.concurrent.Future

trait CouchbaseRecovery {
  this: CouchbaseJournal =>

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] =
    CouchbaseRecovery.asyncReplayMessages(persistenceId, fromSequenceNr, toSequenceNr, max)(replayCallback)

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    CouchbaseRecovery.asyncReadHighestSequenceNr(persistenceId, fromSequenceNr)

  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long =
    CouchbaseRecovery.readHighestSequenceNr(persistenceId, fromSequenceNr)

  def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Unit =
    CouchbaseRecovery.replayMessages(persistenceId, fromSequenceNr, toSequenceNr, max)(replayCallback)


  object CouchbaseRecovery {

    implicit lazy val replayDispatcher = context.system.dispatchers.lookup(config.replayDispatcherId)

    def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Future[Unit] = {
      Future.successful(replayMessages(persistenceId, fromSequenceNr, toSequenceNr, max)(replayCallback))
    }

    def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
      Future.successful(readHighestSequenceNr(persistenceId, fromSequenceNr))
    }

    def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long = {
      val iterator = new PersistentIterator(persistenceId, math.max(1L, fromSequenceNr), Long.MaxValue, Long.MaxValue)
      while (iterator.hasNext) iterator.next()
      iterator.highestSequenceNr
    }

    def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: (PersistentRepr) => Unit): Unit = {
      new PersistentIterator(persistenceId, fromSequenceNr, toSequenceNr, max).foreach(replayCallback)
    }

    /**
      * Iterator over persistents.
      */
    class PersistentIterator(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long) extends Iterator[PersistentRepr] {

      private var highestSequenceNrInternal = 0L

      def highestSequenceNr = highestSequenceNrInternal

      def journalMessageIterator(): Iterator[JournalMessage] = {

        if (max == 0 || fromSequenceNr > toSequenceNr) {
          List.empty[JournalMessage].iterator
        } else {
          val query = bySequenceNr(persistenceId, fromSequenceNr, toSequenceNr)
          val viewRows = bucket.query(query).asScala.iterator
          viewRows.map { viewRow =>
            JournalMessage.deserialize(viewRow.value().asInstanceOf[JsonObject])
          }
        }
      }

      private val messageIterator = journalMessageIterator()
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
