package akka.persistence.couchbase.replay

import java.util.concurrent.TimeUnit

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.event.Logging
import akka.persistence.couchbase.{CouchbaseExtension, CouchbasePersistencyClientContainer}
import com.couchbase.client.java.document.JsonLongDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.view._

import scala.util.{Failure, Try}

trait CouchbaseReplay extends Extension {

  def replayConfig: CouchbaseReplayConfig

  def replay(callback: ReplayCallback, journalMessageIdOption: Option[Long] = None): Unit

  def storeMessageId(identifier: String, journalMessageId: Long): Unit

  def readMessageId(identifier: String): Option[Long]
}

private class DefaultCouchbaseReplay(val system: ExtendedActorSystem) extends CouchbaseReplay
  with CouchbasePersistencyClientContainer {

  private val log = Logging(system, getClass.getName)

  val couchbase = CouchbaseExtension(system)

  override val replayConfig = CouchbaseReplayConfig(system)

  val cluster = client.createCluster(couchbase.environment, replayConfig.nodes) // replayConfig.createCluster(couchbase.environment)

  val replayBucket = client.openBucket(cluster, replayConfig.username, replayConfig.bucketName, replayConfig.bucketPassword) // replayConfig.openBucket(cluster)

  updateJournalDesignDocs()

  private def updateJournalDesignDocs(): Unit = {
    val designDocs = JsonObject.create()
      .put("views", JsonObject.create()
        .put("commits", JsonObject.create()
          .put("map", replayConfig.replayViewCode)
        )
      )

    Try {
      val designDocument = DesignDocument.from("recovery", designDocs)
      couchbase.journalBucket.bucketManager.upsertDesignDocument(designDocument)
    } recoverWith {
      case e =>
        log.error(e, "Updating design documents for recovery")
        Failure(e)
    }
  }

  override def replay(callback: ReplayCallback, journalMessageIdOption: Option[Long]): Unit = {
    system.actorOf(ReplayActor.props(callback)) ! ReplayActor.Recover(journalMessageIdOption)
  }

  override def storeMessageId(identifier: String, journalMessageId: Long): Unit = {
    Try {
      replayBucket.upsert(
        JsonLongDocument.create(s"replayId::$identifier", journalMessageId),
        replayConfig.persistTo,
        replayConfig.replicateTo,
        replayConfig.timeout.toSeconds,
        TimeUnit.SECONDS
      )
    } recoverWith {
      case e =>
        log.error(e, "Store replay id: {}", journalMessageId)
        Failure(e)
    }
  }

  override def readMessageId(identifier: String): Option[Long] = {
    Option(
      replayBucket.get(
        JsonLongDocument.create(s"replayId::$identifier"),
        replayConfig.timeout.toSeconds,
        TimeUnit.SECONDS
      )
    ).map(_.content())
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
