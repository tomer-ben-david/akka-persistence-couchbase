package akka.persistence.couchbase

import java.util.concurrent.{Executors, TimeUnit}

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.event.Logging
import com.couchbase.client.java.{Bucket, Cluster}
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.env.{CouchbaseEnvironment, DefaultCouchbaseEnvironment}
import com.couchbase.client.java.util.Blocking
import com.couchbase.client.java.view.DesignDocument

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Try}

trait Couchbase extends Extension {

  def environment: CouchbaseEnvironment

  def journalBucket: Bucket

  def journalConfig: CouchbaseJournalConfig

  def snapshotStoreBucket: Bucket

  def snapshotStoreConfig: CouchbaseSnapshotStoreConfig
}

class DefaultCouchbase(val system: ExtendedActorSystem) extends Couchbase with CouchbasePersistencyClientContainer {

  private val log = Logging(system, getClass.getName)

  override val journalConfig = CouchbaseJournalConfig(system)

  override val snapshotStoreConfig = CouchbaseSnapshotStoreConfig(system)

  val scheduler = Executors.newSingleThreadScheduledExecutor()

  override def environment = currentEnvironment

  private def recreateEnvironment(): DefaultCouchbaseEnvironment = {
    val funcName = "recreateEnvironment"
    log.info(s"${LogUtils.CBPersistenceKey}.$funcName: about to recreate couchbase environment, current: $currentEnvironment")

    DefaultCouchbaseEnvironment.create()
  }

  private val currentEnvironment: DefaultCouchbaseEnvironment = recreateEnvironment()

  private var journalCluster: Cluster =  Await.result(journalClusterReconnect(), FiniteDuration(6, scala.concurrent.duration.SECONDS)) // journalConfig.createCluster(environment)

  private def journalClusterReconnect(): Future[Cluster] = {
    val funcName = "journalClusterReconnect"
    log.info(s"${LogUtils.CBPersistenceKey}.$funcName: about to reconnect cluster, current cluster: $journalCluster")
    if (journalCluster != null) {
      journalCluster.disconnect()
    }

    val p = Promise[Cluster]()

    scheduler.schedule(new Runnable() {
      val funcName = "journalDisconnectRunnable"

      override def run(): Unit = {
        Try {
        journalCluster = client.createCluster(environment, journalConfig.nodes)
        log.info(s"${LogUtils.CBPersistenceKey}.$funcName: after reconnect cluster, current cluster: $journalCluster")
        p.success(journalCluster)}.recoverWith {
          case t: Throwable =>
            p.failure(t)
            log.error(t, s"${LogUtils.CBPersistenceKey}.$funcName: failed reconnect cluster, current cluster: $journalCluster")
            throw t
        }
      }
    }, 1, TimeUnit.SECONDS)

    p.future

//    journalCluster = client.createCluster(environment, journalConfig.nodes)
//    log.info(s"${LogUtils.CBPersistenceKey}.$funcName: after reconnect cluster, current cluster: $journalCluster")
//    journalCluster
  }

  override def journalBucket: Bucket = currentJournalBucket

  var currentJournalBucket: Bucket = Await.result(reconnectJournalBucket(), FiniteDuration(7, scala.concurrent.duration.SECONDS))

  def reconnectJournalBucket(): Future[Bucket] = {
    val funcName = "reconnectJournalBucket"
    log.info(s"${LogUtils.CBPersistenceKey}.$funcName: reconnecting journal bucket with config: $journalConfig")


    journalClusterReconnect()

    if (currentJournalBucket != null) {
      currentJournalBucket.close()
    }

    val p = Promise[Bucket]()

    scheduler.schedule(new Runnable() {
      val funcName = "journalDisconnectRunnable"

      override def run(): Unit = {
        Try {
          log.info(s"${LogUtils.CBPersistenceKey}.$funcName: reconnected journal bucket before: $currentJournalBucket")
          currentJournalBucket = client.openBucket(journalCluster, journalConfig.username, journalConfig.bucketName, journalConfig.bucketPassword)
          log.info(s"${LogUtils.CBPersistenceKey}.$funcName: reconnected journal bucket after: $currentJournalBucket")
          p.success(currentJournalBucket)
        }.recoverWith {
          case t: Throwable =>
            p.failure(t)
            log.error(t, s"${LogUtils.CBPersistenceKey}.$funcName: failed reconnect cluster, current cluster: $journalCluster")
            throw t
        }
      }
    }, 2, TimeUnit.SECONDS)


    p.future
  }

  private val snapshotStoreCluster = client.createCluster(environment, snapshotStoreConfig.nodes) // snapshotStoreConfig.createCluster(environment)

  override val snapshotStoreBucket = client.openBucket(snapshotStoreCluster, snapshotStoreConfig.username, snapshotStoreConfig.bucketName, snapshotStoreConfig.bucketPassword) // snapshotStoreConfig.openBucket(snapshotStoreCluster)

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
  def updateJournalDesignDocs(): Unit = {

    val designDocs = JsonObject.create()
      .put("views", JsonObject.create()
        .put("by_sequenceNr", JsonObject.create()
          .put("map",
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
          )
        )
      )

    updateDesignDocuments(journalBucket, "journal", designDocs)
  }

  /**
    * Initializes all design documents.
    */
  def updateSnapshotStoreDesignDocs(): Unit = {

    val designDocs = JsonObject.create()
      .put("views", JsonObject.create()
        .put("by_sequenceNr", JsonObject.create()
          .put("map",
            """
              |function (doc) {
              |  if (doc.dataType === 'snapshot-message') {
              |    emit([doc.persistenceId, doc.sequenceNr], null);
              |  }
              |}
            """.stripMargin
          )
        )
        .put("by_timestamp", JsonObject.create()
          .put("map",
            """
              |function (doc) {
              |  if (doc.dataType === 'snapshot-message') {
              |    emit([doc.persistenceId, doc.timestamp], null);
              |  }
              |}
            """.stripMargin
          )
        )
        .put("all", JsonObject.create()
          .put("map",
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

    updateDesignDocuments(snapshotStoreBucket, "snapshots", designDocs)
  }

  def updateDesignDocuments(bucket: Bucket, name: String, raw: JsonObject): Unit = {
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

  var couchbase: DefaultCouchbase = _

  def reconnectJournalBucket(): Bucket = {
    Await.result(couchbase.reconnectJournalBucket(), FiniteDuration(8, scala.concurrent.duration.SECONDS))
  }

  def recreateViews() = {
    couchbase.updateJournalDesignDocs()
    couchbase.updateSnapshotStoreDesignDocs()
  }

  override def createExtension(system: ExtendedActorSystem): Couchbase = {
    couchbase = new DefaultCouchbase(system)
    system.registerOnTermination(couchbase.shutdown())
    couchbase
  }
}
