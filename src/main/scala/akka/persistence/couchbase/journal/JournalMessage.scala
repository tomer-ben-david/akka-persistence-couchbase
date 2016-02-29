package akka.persistence.couchbase.journal

import akka.persistence.couchbase.Message
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}

import scala.collection.JavaConverters._

/**
  * Represents a single persistent message to write to the journal.
  *
  * @param persistenceId of the persistent actor.
  * @param sequenceNr    of message for the persistent actor.
  * @param marker        indicating the meaning of the message.
  * @param message       optional message, depending on the marker.
  */
case class JournalMessage(persistenceId: String,
                          sequenceNr: Long,
                          marker: Marker.Marker,
                          message: Option[Message] = None,
                          tags: Set[String] = Set.empty)

object JournalMessage {

  def serialize(journalMessage: JournalMessage): JsonObject = {
    val jsonObject = JsonObject.create()
      .put("persistenceId", journalMessage.persistenceId)
      .put("sequenceNr", journalMessage.sequenceNr)
      .put("marker", Marker.serialize(journalMessage.marker))

    journalMessage.message.foreach { message =>
      jsonObject.put("message", Message.serialize(message))
    }

    Some(journalMessage.tags).filter(_.nonEmpty).foreach { tags =>
      val tagArray = tags.foldLeft(JsonArray.create())(_ add _)
      jsonObject.put("tags", tagArray)
    }

    jsonObject
  }

  def deserialize(jsonObject: JsonObject): JournalMessage = {
    JournalMessage(
      jsonObject.getString("persistenceId"),
      jsonObject.getLong("sequenceNr"),
      Marker.deserialize(jsonObject.getString("marker")),
      Option(jsonObject.getString("message")).map(Message.deserialize),
      Option(jsonObject.getArray("tags")).map { tagArray =>
        tagArray.iterator().asScala.map(_.asInstanceOf[String]).toSet
      }.getOrElse(Set.empty)
    )
  }
}
