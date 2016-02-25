package akka.persistence.couchbase

import org.apache.commons.codec.binary.Base64
import play.api.libs.json._

/**
 * Wraps message bytes to easily support serialization.
 *
 * @param bytes of the message.
 */
case class Message(bytes: Array[Byte])

object Message {
  implicit val MessageFormat: Format[Message] = Format(Reads.of[String].map(s => apply(Base64.decodeBase64(s))), Writes(a => Writes.of[String].writes(Base64.encodeBase64String(a.bytes))))
}
