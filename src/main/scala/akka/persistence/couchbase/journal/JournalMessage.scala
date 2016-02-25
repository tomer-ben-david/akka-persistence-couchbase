package akka.persistence.couchbase.journal

import akka.persistence.couchbase.Message
import play.api.libs.json._
import play.api.libs.functional.syntax._

/**
  * Represents a single persistent message to write to the journal.
  *
  * @param persistenceId of the persistent actor.
  * @param sequenceNr of message for the persistent actor.
  * @param marker indicating the meaning of the message.
  * @param message optional message, depending on the marker.
  */
case class JournalMessage(persistenceId: String,
                          sequenceNr: Long,
                          marker: Marker.Marker,
                          message: Option[Message] = None,
                          tags: Set[String] = Set.empty)

object JournalMessage {

  implicit class PathAdditions(path: JsPath) {
    def writeEmptySetAsNull[A <: Set[_]](implicit writes: Writes[A]): OWrites[A] =
      OWrites[A] { (a: A) =>
        if (a.isEmpty) Json.obj()
        else JsPath.createObj(path -> writes.writes(a))
      }
  }

  implicit val JournalMessageReads: Reads[JournalMessage] = (
    (__ \ "persistenceId").read[String] and
      (__ \ "sequenceNr").read[Long] and
      (__ \ "marker").read[Marker.Marker] and
      (__ \ "message").readNullable[Message] and
      (__ \ "tags").readNullable[Set[String]].map(_.getOrElse(Set.empty[String]))
    ) (JournalMessage.apply _)

  implicit val JournalMessageWrites: Writes[JournalMessage] = (
    (JsPath \ "persistenceId").write[String] and
      (JsPath \ "sequenceNr").write[Long] and
      (JsPath \ "marker").write[Marker.Marker] and
      (JsPath \ "message").writeNullable[Message] and
      (JsPath \ "tags").writeEmptySetAsNull[Set[String]]
    ) (unlift(JournalMessage.unapply))

  implicit val JournalMessageFormat: Format[JournalMessage] = Format(JournalMessageReads, JournalMessageWrites)
}
