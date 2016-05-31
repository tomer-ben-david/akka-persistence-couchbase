package akka.persistence.couchbase.replay

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, Props}
import akka.persistence.PersistentRepr
import akka.persistence.couchbase.CouchbaseExtension
import akka.persistence.couchbase.replay.ReplayActor.NextPage
import akka.serialization.SerializationExtension
import com.couchbase.client.core.utils.Base64
import com.couchbase.client.java.document.json.JsonArray
import com.couchbase.client.java.view.{Stale, ViewQuery}

import scala.collection.JavaConverters._

class ReplayActor(callback: ReplayCallback) extends Actor {

  type JLong = java.lang.Long

  val couchbase = CouchbaseExtension(context.system)

  val config = CouchbaseReplayConfig(context.system)

  val serialization = SerializationExtension(context.system)

  val bucket = couchbase.journalBucket

  val query = ViewQuery
    .from("recovery", "commits")
    .includeDocs(false)
    .limit(config.batchSize)

  override def receive: Receive = {
    case ReplayActor.Recover(journalMessageId) =>
      query.stale(Stale.FALSE)
      processNext(ReplayCursor(journalMessageId))
      query.stale(Stale.TRUE)
  }

  def recovering(cursor: ReplayCursor): Receive = {
    case NextPage =>
      processNext(cursor)
  }

  def processNext(cursor: ReplayCursor): Unit = {
    processBatch(cursor) match {
      case identical if identical == cursor =>
        callback.onReplayComplete(cursor.journalMessageId.getOrElse(Long.MinValue))
        context.stop(self)

      case next =>
        context.become(recovering(next))
        self ! NextPage
    }
  }

  def processBatch(cursor: ReplayCursor): ReplayCursor = {
    cursor.journalMessageId.fold(query) { journalMessageId =>
      query
        .skip(1)
        .startKey(
          JsonArray.from(
            cursor.journalMessageId.getOrElse(Long.MinValue).asInstanceOf[JLong],
            cursor.sequenceNrOption.getOrElse(Long.MinValue).asInstanceOf[JLong]
          )
        )
    }

    cursor.docIdOption.foreach { docId =>
      query.startKeyDocId(docId)
    }

    val viewRowIterator = bucket.query(query, config.timeout.toSeconds, TimeUnit.SECONDS).iterator().asScala

    viewRowIterator.foldLeft(cursor) { case (acc, viewRow) =>

      // Replay metadata
      val key = viewRow.key().asInstanceOf[JsonArray]
      val journalMessageId = key.getLong(0)
      val sequenceNr = key.getLong(1)

      // Persistent data
      val message = viewRow.value().asInstanceOf[String]
      val messageBytes = Base64.decode(message)
      val persistentReprAttempt = serialization.deserialize(messageBytes, classOf[PersistentRepr])
      callback.replay(journalMessageId, persistentReprAttempt)

      // Update for next
      cursor.update(journalMessageId, sequenceNr, viewRow.id())
    }
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    callback.onReplayFailed(reason)
    context.stop(self)
    super.preRestart(reason, message)
  }
}

object ReplayActor {

  def props(callback: ReplayCallback): Props = Props(classOf[ReplayActor], callback)

  case class Recover(journalMessageIdOption: Option[Long])

  case object NextPage

}
