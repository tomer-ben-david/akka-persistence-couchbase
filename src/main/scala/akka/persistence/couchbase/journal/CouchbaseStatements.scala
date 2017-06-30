package akka.persistence.couchbase.journal

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging}
import akka.persistence.couchbase.CouchbaseJournalConfig
import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonArray
import com.couchbase.client.java.view._
import rx.Observable

import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Try}

trait CouchbaseStatements extends Actor with ActorLogging {

  def config: CouchbaseJournalConfig

  def bucket: Bucket

  implicit def executionContext: ExecutionContext

  def bySequenceNr(persistenceId: String, from: Long, to: Long): ViewQuery = {
    ViewQuery
      .from("journal", "by_sequenceNr")
      .stale(config.stale)
      .startKey(JsonArray.from(persistenceId, from.asInstanceOf[AnyRef]))
      .endKey(JsonArray.from(persistenceId, to.asInstanceOf[AnyRef]))
  }

  /**
    * Adds all messages in a single atomically updated batch.
    */
  def executeBatch(messages: Seq[JournalMessage]): Try[Unit] = {
    nextKey(JournalMessageBatch.name).flatMap { key =>
      Try {
        val batch = JournalMessageBatch.create(messages)

        val jsonObject = JournalMessageBatch.serialize(batch)
        val jsonDocument = JsonDocument.create(key, jsonObject)
        bucket.insert(
          jsonDocument,
          config.persistTo,
          config.replicateTo,
          config.timeout.toSeconds,
          TimeUnit.SECONDS
        )
        log.debug("Wrote batch: {}", key)
      } recoverWith {
        case e =>
          log.error(e, "Writing batch: {}", key)
          Failure(e)
      }
    }
  }

  /**
    * removes a batch of journal messages
   */
  def deleteBatch(sequenceIds: Seq[Long]): Try[Unit] = {
    Try {
      val keyBatch = sequenceIds collect { case i => s"${JournalMessageBatch.name}-$i" }

      val requestResults: Seq[Observable[JsonDocument]] = keyBatch.map(key => bucket.async()
                                                                                    .remove(key,
                                                                                            config.persistTo,
                                                                                            config.replicateTo))

      def mergeObservables(res: Observable[JsonDocument], i:Observable[JsonDocument]) = {
        res.mergeWith(i)
      }

      val mergedObservables: Observable[JsonDocument] = requestResults.fold(Observable.empty()) (mergeObservables)

//      wait until all the observable are done
      val result = mergedObservables.toList.toBlocking.single()
    } recoverWith {
      case e =>
        log.error(e, "Deleting batch")
        Failure(e)
    }
  }

  /**
    * Generates a new key with the given base name.
    *
    * Couchbase guarantees the key is unique within the cluster.
    */
  def nextKey(name: String): Try[String] = {
    Try {
      val counterKey = s"counter::$name"
      val counter = bucket.counter(counterKey, 1L, 0L).content()
      s"$name-$counter"
    }
  }
}
