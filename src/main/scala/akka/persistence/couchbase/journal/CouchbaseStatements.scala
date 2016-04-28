package akka.persistence.couchbase.journal

import akka.actor.{Actor, ActorLogging}
import com.couchbase.client.java.{Bucket, PersistTo, ReplicateTo}
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonArray
import com.couchbase.client.java.view._

import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

trait CouchbaseStatements extends Actor with ActorLogging {

  def bucket: Bucket

  implicit def executionContext: ExecutionContext

  def bySequenceNr(persistenceId: String, from: Long, to: Long) = {
    ViewQuery
      .from("journal", "by_sequenceNr")
      .stale(Stale.FALSE)
      .startKey(JsonArray.from(persistenceId, from.asInstanceOf[AnyRef]))
      .endKey(JsonArray.from(persistenceId, to.asInstanceOf[AnyRef]))
  }

  /**
    * Adds all messages in a single atomically updated batch.
    */
  def executeBatch(messages: Seq[JournalMessage]): Future[Unit] = {
    val batch = JournalMessageBatch.create(messages)
    val keyFuture = nextKey(JournalMessageBatch.name)

    keyFuture.map { key =>
      Try {
        val jsonObject = JournalMessageBatch.serialize(batch)
        val jsonDocument = JsonDocument.create(key, jsonObject)
        bucket.insert(
          jsonDocument,
          PersistTo.NONE,
          ReplicateTo.NONE
        )
        log.debug("Wrote batch: {}", key)
      } recoverWith {
        case e => log.error(e, "Writing batch: {}", key)
          Failure(e)
      }
    }
  }

  /**
    * Generates a new key with the given base name.
    *
    * Couchbase guarantees the key is unique within the cluster.
    */
  def nextKey(name: String): Future[String] = {
    val counterKey = s"counter::$name"

    val counter = bucket.counter(counterKey, 1L, 0L).content()
    Future.successful(s"$name-$counter")
  }
}
