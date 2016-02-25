package akka.persistence.couchbase

import java.util.concurrent.TimeUnit

import akka.actor.{ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider}
import akka.event.Logging
import com.couchbase.client.java.Bucket
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment
import com.couchbase.client.java.util.Blocking

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
}

object CouchbaseExtension extends ExtensionId[Couchbase] with ExtensionIdProvider {

  override def lookup(): ExtensionId[Couchbase] = CouchbaseExtension

  override def createExtension(system: ExtendedActorSystem): Couchbase = {
    val couchbase = new DefaultCouchbase(system)
    system.registerOnTermination(couchbase.shutdown())
    couchbase
  }
}
