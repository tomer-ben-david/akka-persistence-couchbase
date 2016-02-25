package akka.persistence.couchbase.snapshot

import akka.persistence.SnapshotMetadata

case class SnapshotMessageKey private(value: String) extends Proxy {
  override def self: Any = value
}

object SnapshotMessageKey {

  def create(persistenceId: String, sequenceNr: Long) = SnapshotMessageKey(s"$persistenceId-$sequenceNr")

  def fromMetadata(metadata: SnapshotMetadata): SnapshotMessageKey = create(metadata.persistenceId, metadata.sequenceNr)
}
