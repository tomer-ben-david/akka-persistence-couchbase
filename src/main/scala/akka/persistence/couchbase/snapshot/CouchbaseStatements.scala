package akka.persistence.couchbase.snapshot

import akka.actor.{Actor, ActorLogging}
import akka.persistence.couchbase.CouchbaseSnapshotStoreConfig
import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonArray
import com.couchbase.client.java.view.ViewQuery

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

trait CouchbaseStatements extends Actor with ActorLogging {

  def config: CouchbaseSnapshotStoreConfig

  def bucket: Bucket

  implicit def executionContext: ExecutionContext

  def bySequenceNr(persistenceId: String, maxSequenceNr: Long): ViewQuery = {
    ViewQuery
      .from("snapshots", "by_sequenceNr")
      .stale(config.stale)
      .descending(true)
      .startKey(JsonArray.from(persistenceId, maxSequenceNr.asInstanceOf[AnyRef]))
      .endKey(JsonArray.from(persistenceId, Long.MinValue.asInstanceOf[AnyRef]))
  }

  def byTimestamp(persistenceId: String, maxTimestamp: Long): ViewQuery = {
    ViewQuery
      .from("snapshots", "by_timestamp")
      .stale(config.stale)
      .descending(true)
      .startKey(JsonArray.from(persistenceId, maxTimestamp.asInstanceOf[AnyRef]))
      .endKey(JsonArray.from(persistenceId, Long.MinValue.asInstanceOf[AnyRef]))
  }

  def all(persistenceId: String): ViewQuery = {
    ViewQuery
      .from("snapshots", "all")
      .stale(config.stale)
      .descending(true)
      .key(persistenceId)
  }

  /**
    * Saves a snapshot.
    */
  def executeSave(snapshotMessage: SnapshotMessage): Try[Unit] = {
    Try(SnapshotMessageKey.fromMetadata(snapshotMessage.metadata).value).flatMap { key =>
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
