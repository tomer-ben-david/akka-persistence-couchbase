package akka.persistence.couchbase

import java.util.concurrent.TimeUnit

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.event.Logging
import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment
import com.couchbase.client.java.util.Blocking
import com.couchbase.client.java.view.DesignDocument
import play.api.libs.json.Json

import scala.util.{Failure, Try}

trait Couchbase extends Extension {

  def journalBucket: Bucket

  def journalConfig: CouchbaseJournalConfig

  def snapshotStoreBucket: Bucket

  def snapshotStoreConfig: CouchbaseSnapshotStoreConfig
}

private class DefaultCouchbase(val system: ExtendedActorSystem) extends Couchbase {

  private val log = Logging(system, getClass.getName)

  override val journalConfig = CouchbaseJournalConfig(system)

  override val snapshotStoreConfig = CouchbaseSnapshotStoreConfig(system)

  private val environment = DefaultCouchbaseEnvironment.create()

  private val journalCluster = journalConfig.createCluster(environment)

  override val journalBucket = journalConfig.openBucket(journalCluster)

  private val snapshotStoreCluster = snapshotStoreConfig.createCluster(environment)

  override val snapshotStoreBucket = snapshotStoreConfig.openBucket(snapshotStoreCluster)

  updateJournalDesignDocs()
  updateSnapshotStoreDesignDocs()

  def shutdown(): Unit = {
    attemptSafely("Closing journal bucket")(journalBucket.close())

    attemptSafely("Shutting down journal cluster")(journalCluster.disconnect())

    attemptSafely("Closing snapshot store bucket")(snapshotStoreBucket.close())
    attemptSafely("Shutting down snapshot store cluster")(snapshotStoreCluster.disconnect())

    attemptSafely("Shutting down environment") {
      Blocking.blockForSingle(environment.shutdownAsync().single(), 30, TimeUnit.SECONDS)
    }
  }

  private def attemptSafely(message: String)(block: => Unit): Unit = {
    log.debug(message)

    Try(block) recoverWith {
      case e =>
        log.error(e, message)
        Failure(e)
    }
  }

  /**
    * Initializes all design documents.
    */
  private def updateJournalDesignDocs(): Unit = {
    val journalDesignDocumentJson = Json.obj(
      "views" -> Json.obj(
        "by_sequenceNr" -> Json.obj(
          "map" ->
            """
              |function (doc, meta) {
              |  if (doc.dataType === 'journal-messages') {
              |    var messages = doc.messages;
              |    for (var i = 0, l = messages.length; i < l; i++) {
              |      var message = messages[i];
              |      emit([message.persistenceId, message.sequenceNr], message);
              |    }
              |  }
              |}
            """.stripMargin
        ),
        "by_revision" -> Json.obj(
          "map" ->
            """
              |function (doc, meta) {
              |  if (doc.dataType === 'journal-messages') {
              |    var messages = doc.messages;
              |    for (var i = 0, l = messages.length; i < l; i++) {
              |      var message = messages[i];
              |      emit([parseInt(meta.id.substring(17)), message.persistenceId, message.sequenceNr], message);
              |    }
              |  }
              |}
            """.stripMargin
        )
      )
    )

    updateDesignDocuments(journalBucket, "journal", JsonObject.fromJson(journalDesignDocumentJson.toString()))
  }

  /**
    * Initializes all design documents.
    */
  private def updateSnapshotStoreDesignDocs(): Unit = {
    val snapshotsDesignDocumentJson = Json.obj(
      "views" -> Json.obj(
        "by_sequenceNr" -> Json.obj(
          "map" ->
            """
              |function (doc) {
              |  if (doc.dataType === 'snapshot-message') {
              |    emit([doc.persistenceId, doc.sequenceNr], null);
              |  }
              |}
            """.stripMargin
        ),
        "by_timestamp" -> Json.obj(
          "map" ->
            """
              |function (doc) {
              |  if (doc.dataType === 'snapshot-message') {
              |    emit([doc.persistenceId, doc.timestamp], null);
              |  }
              |}
            """.stripMargin
        ),
        "all" -> Json.obj(
          "map" ->
            """
              |function (doc) {
              |  if (doc.dataType === 'snapshot-message') {
              |    emit(doc.persistenceId, null);
              |  }
              |}
            """.stripMargin
        )
      )
    )

    updateDesignDocuments(snapshotStoreBucket, "snapshots", JsonObject.fromJson(snapshotsDesignDocumentJson.toString()))
  }

  private def updateDesignDocuments(bucket: Bucket, name: String, raw: JsonObject): Unit = {
    Try {
      val designDocument = DesignDocument.from(name, raw)
      bucket.bucketManager.upsertDesignDocument(designDocument)
    } recoverWith {
      case e =>
        log.error(e, "Update design docs with name: {}", name)
        Failure(e)
    }
  }
}

object CouchbaseExtension extends ExtensionId[Couchbase] with ExtensionIdProvider {

  override def lookup(): ExtensionId[Couchbase] = CouchbaseExtension

  override def createExtension(system: ExtendedActorSystem): Couchbase = {
    val couchbase = new DefaultCouchbase(system)
    system.registerOnTermination(couchbase.shutdown())
    couchbase
  }
}
