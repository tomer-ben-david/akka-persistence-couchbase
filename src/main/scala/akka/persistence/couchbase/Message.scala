package akka.persistence.couchbase

import org.apache.commons.codec.binary.Base64

/**
  * Wraps message bytes to easily support serialization.
  *
  * @param bytes of the message.
  */
case class Message(bytes: Array[Byte])

object Message {

  def deserialize(s: String): Message = apply(Base64.decodeBase64(s))

  def serialize(message: Message): String = Base64.encodeBase64String(message.bytes)
}
