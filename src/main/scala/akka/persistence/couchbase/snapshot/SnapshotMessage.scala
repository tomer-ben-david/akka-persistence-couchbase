package akka.persistence.couchbase.snapshot

import akka.persistence.SnapshotMetadata
import akka.persistence.couchbase.Message
import com.couchbase.client.java.document.json.JsonObject

case class SnapshotMessage private(dataType: String,
                                   persistenceId: String,
                                   sequenceNr: Long,
                                   timestamp: Long,
                                   message: Message) {

  lazy val metadata = new SnapshotMetadata(persistenceId, sequenceNr, timestamp)
}

object SnapshotMessage {

  val name = "snapshot-message"

  def create(metadata: SnapshotMetadata, message: Message) = {
    SnapshotMessage(name, metadata.persistenceId, metadata.sequenceNr, metadata.timestamp, message)
  }

  def serialize(snapshotMessage: SnapshotMessage): JsonObject = {
    JsonObject.create()
      .put("dataType", snapshotMessage.dataType)
      .put("persistenceId", snapshotMessage.persistenceId)
      .put("sequenceNr", snapshotMessage.sequenceNr)
      .put("timestamp", snapshotMessage.timestamp)
      .put("message", Message.serialize(snapshotMessage.message))
  }

  def deserialize(jsonObject: JsonObject): SnapshotMessage = {
    SnapshotMessage(
      jsonObject.getString("dataType"),
      jsonObject.getString("persistenceId"),
      jsonObject.getLong("sequenceNr"),
      jsonObject.getLong("timestamp"),
      Message.deserialize(jsonObject.getString("message"))
    )
  }
}
