package akka.persistence.couchbase.journal

import play.api.libs.json.{Json, Format}

/**
 * Represents a batch of messages to write to the journal.
 *
 * Using a batch representation allows us to atomically commit an entire batch.
 * @param dataType of the messages in the batch.
 * @param messages in the batch.
 */
case class JournalMessageBatch private(dataType: String, messages: Seq[JournalMessage])

object JournalMessageBatch {

  val name = "journal-messages"

  implicit val jsonFormat: Format[JournalMessageBatch] = Json.format[JournalMessageBatch]

  def create(messages: Seq[JournalMessage]) = JournalMessageBatch(name, messages)
}
