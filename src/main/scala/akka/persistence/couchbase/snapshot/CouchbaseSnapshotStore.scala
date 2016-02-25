package akka.persistence.couchbase.snapshot

import akka.actor.ActorLogging
import akka.persistence.couchbase.{CouchbaseExtension, Message}
import akka.persistence.serialization.Snapshot
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import akka.serialization.SerializationExtension
import com.couchbase.client.java.view.ViewRow
import play.api.libs.json.Json

import scala.concurrent.Future
import scala.util.Try

import scala.collection.JavaConverters._

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
   * @param criteria selection criteria for loading.
   */
  override def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    Future.successful {
      query(persistenceId, criteria, 1).headOption.map { snapshotMessage =>
        val metadata = SnapshotMetadata(snapshotMessage.persistenceId, snapshotMessage.sequenceNr, snapshotMessage.timestamp)
        val snapshot = deserialize(snapshotMessage.message)
        SelectedSnapshot(metadata, snapshot.data)
      }
    }
  }

  def query(persistenceId: String, criteria: SnapshotSelectionCriteria, limit: Int): Iterable[SnapshotMessage] = {

    def toSnapshotMessage(row: ViewRow) = Json.parse(row.document.content().toString).as[SnapshotMessage]

    if (criteria.equals(SnapshotSelectionCriteria.None)) {
      List.empty[SnapshotMessage]
    } else {
      val latest = SnapshotSelectionCriteria.Latest

      if (criteria == latest) {
        bucket.query(all(persistenceId).limit(limit)).asScala.map(toSnapshotMessage)
      } else if (criteria.maxSequenceNr == latest.maxSequenceNr && criteria.maxTimestamp != latest.maxTimestamp) {
        bucket.query(byTimestamp(persistenceId, criteria.maxTimestamp).limit(limit)).asScala.map(toSnapshotMessage)
      } else if (criteria.maxSequenceNr != latest.maxSequenceNr && criteria.maxTimestamp == latest.maxTimestamp) {
        bucket.query(bySequenceNr(persistenceId, criteria.maxSequenceNr).limit(limit)).asScala.map(toSnapshotMessage)
      } else if (criteria.maxSequenceNr != latest.maxSequenceNr && criteria.maxTimestamp != latest.maxTimestamp) {
        bucket.query(bySequenceNr(persistenceId, criteria.maxSequenceNr)).asScala.map(toSnapshotMessage).filter(_.timestamp <= criteria.maxTimestamp).take(limit)
      } else {
        throw new IllegalArgumentException(s"Unexpected criteria $criteria")
      }
    }
  }

  /**
   * Plugin API: asynchronously saves a snapshot.
   *
   * @param metadata snapshot metadata.
   * @param snapshot snapshot.
   */
  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    val message = serialize(Snapshot(snapshot))
    val snapshotMessage = SnapshotMessage.create(metadata, message)
    executeSave(snapshotMessage)
  }

  override def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = {
    Future.fromTry[Unit](Try {
      bucket.remove(SnapshotMessageKey.fromMetadata(metadata).value)
    })
  }

  override def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = {
    Future.fromTry[Unit](Try {
      query(persistenceId, criteria, Integer.MAX_VALUE).foreach { snapshotMessage =>
        bucket.remove(SnapshotMessageKey.fromMetadata(snapshotMessage.metadata).value)
      }
    })
  }

  /**
   * Create design docs.
   */
  override def preStart(): Unit = {
    super.preStart()
    initDesignDocs()
  }

  /**
   * Serializes a persistent to a message.
   */
  def serialize(snapshot: Snapshot): Message = Message(serialization.findSerializerFor(snapshot).toBinary(snapshot))

  /**
   * Deserializes a message to a persistent.
   */
  def deserialize(message: Message): Snapshot = serialization.deserialize(message.bytes, classOf[Snapshot]).get
}
