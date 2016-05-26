package akka.persistence.couchbase.snapshot

import java.util.concurrent.TimeUnit

import akka.actor.ActorLogging
import akka.persistence.couchbase.{CouchbaseExtension, Message}
import akka.persistence.serialization.Snapshot
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import akka.serialization.SerializationExtension
import com.couchbase.client.java.view.ViewRow

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Try

class CouchbaseSnapshotStore extends SnapshotStore with CouchbaseStatements with ActorLogging {

  implicit val executionContext = context.dispatcher

  val couchbase = CouchbaseExtension(context.system)
  val serialization = SerializationExtension(context.system)

  def config = couchbase.snapshotStoreConfig

  def bucket = couchbase.snapshotStoreBucket

  /**
    * Plugin API: asynchronously loads a snapshot.
    *
    * @param persistenceId processor id.
    * @param criteria      selection criteria for loading.
    */
  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    Future.fromTry {
      Try {
        query(persistenceId, criteria, 1).headOption.map { snapshotMessage =>
          val metadata = SnapshotMetadata(snapshotMessage.persistenceId, snapshotMessage.sequenceNr, snapshotMessage.timestamp)
          val snapshot = serialization.serializerFor(classOf[Snapshot]).fromBinary(snapshotMessage.message.bytes)
          SelectedSnapshot(metadata, snapshot.asInstanceOf[Snapshot].data)
        }
      }
    }
  }

  def query(persistenceId: String, criteria: SnapshotSelectionCriteria, limit: Int): Iterable[SnapshotMessage] = {

    def toSnapshotMessage(row: ViewRow) = SnapshotMessage.deserialize(row.document.content())

    if (criteria.equals(SnapshotSelectionCriteria.None)) {
      List.empty[SnapshotMessage]
    } else {
      val latest = SnapshotSelectionCriteria.Latest

      if (criteria == latest) {
        bucket.query(all(persistenceId).limit(limit), config.timeout.toSeconds, TimeUnit.SECONDS).asScala.map(toSnapshotMessage)
      } else if (criteria.maxSequenceNr == latest.maxSequenceNr && criteria.maxTimestamp != latest.maxTimestamp) {
        bucket.query(byTimestamp(persistenceId, criteria.maxTimestamp).limit(limit), config.timeout.toSeconds, TimeUnit.SECONDS).asScala.map(toSnapshotMessage)
      } else if (criteria.maxSequenceNr != latest.maxSequenceNr && criteria.maxTimestamp == latest.maxTimestamp) {
        bucket.query(bySequenceNr(persistenceId, criteria.maxSequenceNr).limit(limit), config.timeout.toSeconds, TimeUnit.SECONDS).asScala.map(toSnapshotMessage)
      } else if (criteria.maxSequenceNr != latest.maxSequenceNr && criteria.maxTimestamp != latest.maxTimestamp) {
        bucket.query(bySequenceNr(persistenceId, criteria.maxSequenceNr), config.timeout.toSeconds, TimeUnit.SECONDS).asScala.map(toSnapshotMessage).filter(_.timestamp <= criteria.maxTimestamp).take(limit)
      } else {
        throw new IllegalArgumentException(s"Unexpected criteria $criteria")
      }
    }
  }

  /**
    * Plugin API: asynchronously saves a snapshot.
    *
    * @param metadata snapshot metadata.
    * @param data     snapshot.
    */
  override def saveAsync(metadata: SnapshotMetadata, data: Any): Future[Unit] = {
    Future.fromTry[Unit](
      Try {
        val snapshot = Snapshot(data)
        val message = Message(serialization.findSerializerFor(snapshot).toBinary(snapshot))
        SnapshotMessage.create(metadata, message)
      } flatMap executeSave
    )
  }

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    Future.fromTry[Unit](
      Try {
        bucket.remove(SnapshotMessageKey.fromMetadata(metadata).value)
      }
    )
  }

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    Future.fromTry[Unit](
      Try {
        query(persistenceId, criteria, Integer.MAX_VALUE).foreach { snapshotMessage =>
          bucket.remove(SnapshotMessageKey.fromMetadata(snapshotMessage.metadata).value)
        }
      }
    )
  }
}
