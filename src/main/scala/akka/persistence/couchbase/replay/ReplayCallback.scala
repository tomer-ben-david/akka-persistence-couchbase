package akka.persistence.couchbase.replay

import akka.persistence.PersistentRepr

import scala.util.Try

trait ReplayCallback {

  def replay(journalMessageId: Long, persistentReprAttempt: Try[PersistentRepr]): Unit = {}

  def onReplayComplete(journalMessageId: Long): Unit = {}

  def onReplayFailed(reason: Throwable): Unit = {}
}
