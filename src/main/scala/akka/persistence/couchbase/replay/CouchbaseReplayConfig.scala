package akka.persistence.couchbase.replay

import akka.actor.ActorSystem
import akka.persistence.couchbase.{CouchbasePluginConfig, DefaultCouchbasePluginConfig}
import com.typesafe.config.Config

trait CouchbaseReplayConfig extends CouchbasePluginConfig {

  def batchSize: Int

  def replayViewCode: String
}

object CouchbaseReplayConfig {
  def apply(system: ActorSystem) = {
    new DefaultCouchbaseReplayConfig(system.settings.config.getConfig("couchbase-replay"))
  }
}

class DefaultCouchbaseReplayConfig(config: Config)
  extends DefaultCouchbasePluginConfig(config)
    with CouchbaseReplayConfig {

  override val batchSize: Int = config.getInt("batchSize")

  override val replayViewCode: String = config.getString("replay-view-code")
}
