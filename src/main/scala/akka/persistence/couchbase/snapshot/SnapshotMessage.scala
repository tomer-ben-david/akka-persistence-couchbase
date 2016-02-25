package akka.persistence.couchbase.snapshot

import akka.persistence.SnapshotMetadata
import akka.persistence.couchbase.Message
import play.api.libs.json.{Format, Json}

case class SnapshotMessage private(dataType: String,
                                   persistenceId: String,
                                   sequenceNr: Long,
                                   timestamp: Long,
                                   message: Message) {

  lazy val metadata = new SnapshotMetadata(persistenceId, sequenceNr, timestamp)
}

object SnapshotMessage {
  val Name = "snapshot-message"

  implicit val SnapshotMessageFormat: Format[SnapshotMessage] = Json.format[SnapshotMessage]

  def create(metadata: SnapshotMetadata, message: Message) = {
    SnapshotMessage(Name, metadata.persistenceId, metadata.sequenceNr, metadata.timestamp, message)
  }
}
