package akka.persistence.couchbase.replay

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration

trait CouchbaseReplayConfig {

  def batchSize: Int

  def timeout: FiniteDuration

  def replayViewCode: String
}

object CouchbaseReplayConfig {
  def apply(system: ActorSystem) = {
    new DefaultCouchbaseReplayConfig(system.settings.config.getConfig("couchbase-replay"))
  }
}

class DefaultCouchbaseReplayConfig(config: Config) extends CouchbaseReplayConfig {

  override val batchSize: Int = config.getInt("batchSize")

  override val timeout: FiniteDuration = FiniteDuration(config.getDuration("timeout").getSeconds, TimeUnit.SECONDS)

  override val replayViewCode: String = config.getString("replay-view-code")
}
