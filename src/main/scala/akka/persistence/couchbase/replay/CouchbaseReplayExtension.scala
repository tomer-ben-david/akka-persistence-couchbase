package akka.persistence.couchbase.replay

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.event.Logging
import akka.persistence.couchbase.CouchbaseExtension
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.view._

import scala.util.{Failure, Try}

trait CouchbaseReplay extends Extension {

  def replay(callback: ReplayCallback, journalMessageIdOption: Option[Long] = None): Unit
}

private class DefaultCouchbaseReplay(val system: ExtendedActorSystem) extends CouchbaseReplay {

  private val log = Logging(system, getClass.getName)

  val couchbase = CouchbaseExtension(system)

  val bucket = couchbase.journalBucket

  val config = CouchbaseReplayConfig(system)

  updateJournalDesignDocs()

  private def updateJournalDesignDocs(): Unit = {
    val designDocs = JsonObject.create()
      .put("views", JsonObject.create()
        .put("commits", JsonObject.create()
          .put("map", config.replayViewCode)
        )
      )

    Try {
      val designDocument = DesignDocument.from("recovery", designDocs)
      bucket.bucketManager.upsertDesignDocument(designDocument)
    } recoverWith {
      case e =>
        log.error(e, "Updating design documents for recovery")
        Failure(e)
    }
  }

  override def replay(callback: ReplayCallback, journalMessageIdOption: Option[Long]): Unit = {
    system.actorOf(ReplayActor.props(callback)) ! ReplayActor.Recover(journalMessageIdOption.getOrElse(Long.MinValue))
  }
}

/**
  * Extension that allows recovery of all events written to the event journal.
  */
object CouchbaseReplayExtension extends ExtensionId[CouchbaseReplay] with ExtensionIdProvider {

  override def lookup(): ExtensionId[CouchbaseReplay] = CouchbaseReplayExtension

  override def createExtension(system: ExtendedActorSystem): CouchbaseReplay = {
    new DefaultCouchbaseReplay(system)
  }
}
