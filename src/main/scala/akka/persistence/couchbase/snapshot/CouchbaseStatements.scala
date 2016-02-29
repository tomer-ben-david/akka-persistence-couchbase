package akka.persistence.couchbase.snapshot

import akka.actor.{Actor, ActorLogging}
import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonArray
import com.couchbase.client.java.view.{Stale, ViewQuery}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

trait CouchbaseStatements extends Actor with ActorLogging {

  def bucket: Bucket

  implicit def executionContext: ExecutionContext

  def bySequenceNr(persistenceId: String, maxSequenceNr: Long) = {
    ViewQuery
      .from("snapshots", "by_sequenceNr")
      .stale(Stale.FALSE)
      .descending(true)
      .startKey(JsonArray.from(persistenceId, maxSequenceNr.asInstanceOf[AnyRef]))
      .endKey(JsonArray.from(persistenceId, Long.MinValue.asInstanceOf[AnyRef]))
  }

  def byTimestamp(persistenceId: String, maxTimestamp: Long) = {
    ViewQuery
      .from("snapshots", "by_timestamp")
      .stale(Stale.FALSE)
      .descending(true)
      .startKey(JsonArray.from(persistenceId, maxTimestamp.asInstanceOf[AnyRef]))
      .endKey(JsonArray.from(persistenceId, Long.MinValue.asInstanceOf[AnyRef]))
  }

  def all(persistenceId: String) = {
    ViewQuery
      .from("snapshots", "all")
      .stale(Stale.FALSE)
      .descending(true)
      .key(persistenceId)
  }

  /**
    * Saves a snapshot.
    */
  def executeSave(snapshotMessage: SnapshotMessage): Future[Unit] = {
    Future.successful {
      val key = SnapshotMessageKey.fromMetadata(snapshotMessage.metadata).value

      Try {
        val jsonObject = SnapshotMessage.serialize(snapshotMessage)
        val jsonDocument = JsonDocument.create(key, jsonObject)
        bucket.upsert(jsonDocument)
        log.debug("Wrote snapshot: {}", key)
      } recoverWith {
        case e =>
          log.error(e, "Writing snapshot: {}", key)
          Failure(e)
      }
    }
  }
}
