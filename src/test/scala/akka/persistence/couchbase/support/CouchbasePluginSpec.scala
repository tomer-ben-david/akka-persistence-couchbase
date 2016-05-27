package akka.persistence.couchbase.support

import akka.actor.ActorSystem
import akka.persistence.couchbase.{LoggingConfig, CouchbaseExtension}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.concurrent.Await
import scala.concurrent.duration._

object CouchbasePluginSpec {

  val config = ConfigFactory.parseString(
    """
      |akka {
      |  persistence {
      |    journal {
      |      plugin = "couchbase-journal"
      |    }
      |
      |    snapshot-store {
      |      plugin =  "couchbase-snapshot-store"
      |    }
      |
      |    journal-plugin-fallback {
      |      replay-filter {
      |        mode = warn
      |      }
      |    }
      |  }
      |
      |  test.single-expect-default = 10s
      |  loglevel = WARNING
      |  log-dead-letters = 0
      |  log-dead-letters-during-shutdown = off
      |  test.single-expect-default = 10s
      |}
      |
      |couchbase-replay {
      |
      |  batchSize = "10"
      |}
    """.stripMargin)
}

trait CouchbasePluginSpec extends Suite with BeforeAndAfterAll {

  System.setProperty("java.util.logging.config.class", classOf[LoggingConfig].getName)

  def system: ActorSystem

  def couchbase = CouchbaseExtension(system)

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    assert(couchbase.journalBucket.bucketManager.flush())
    assert(couchbase.snapshotStoreBucket.bucketManager.flush())
  }

  override protected def afterAll(): Unit = {
    Await.result(system.terminate(), 10.seconds)
    super.afterAll()
  }
}
