package akka.persistence.couchbase.journal

import play.api.libs.json._

/**
 * Allows operations on persisted messages without the need to update them.
 */
object Marker {

  sealed trait Marker {
    def value: String
  }

  sealed trait MarkerCompanion {
    def parse(v: String): Option[Marker]
  }

  /**
   * Represents a message written to the journal. 
   */
  case object Message extends Marker with MarkerCompanion {
    override val value = "M"

    override def parse(v: String) = if (v == value) Some(Message) else None
  }

  /**
   * Represents a message written to the journal that is marked as deleted.
   */
  case object MessageDeleted extends Marker with MarkerCompanion {
    override val value = "D"

    override def parse(v: String) = if (v == value) Some(MessageDeleted) else None
  }

  private val companions: List[MarkerCompanion] = List(Message, MessageDeleted)

  def parse(s: String): Option[Marker] = companions.flatMap(_ parse s).headOption

  implicit val MarkerFormat: Format[Marker] = Format(Reads.of[String].map(s => parse(s).get), Writes(a => Writes.of[String].writes(a.value)))
}
