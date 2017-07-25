package akka.persistence.couchbase.journal

import com.couchbase.client.java.document.json.{JsonArray, JsonObject}

import scala.collection.JavaConverters._

/**
  * Represents a batch of messages to write to the journal.
  *
  * Using a batch representation allows us to atomically commit an entire batch.
  *
  * @param dataType of the messages in the batch.
  * @param messages in the batch.
  */
case class JournalMessageBatch private(dataType: String, messages: Seq[JournalMessage])

object JournalMessageBatch {

  val name = "journal-messages"

  def create(messages: Seq[JournalMessage]) = JournalMessageBatch(name, messages)

  def serialize(journalMessageBatch: JournalMessageBatch): JsonObject = {
    JsonObject.create()
      .put("dataType", journalMessageBatch.dataType)
      .put("messages", journalMessageBatch.messages.map(JournalMessage.serialize).foldLeft(JsonArray.create())(_ add _))
  }

  def deserialize(jsonObject: JsonObject, documentId: String): JournalMessageBatch = {

    JournalMessageBatch(
      jsonObject.getString("dataType"),
      jsonObject.getArray("messages").iterator().asScala.map {
        message => JournalMessage.deserialize(message.asInstanceOf[JsonObject], documentId)
      }.toSeq
    )
  }
}
