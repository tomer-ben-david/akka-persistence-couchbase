package akka.persistence.couchbase.journal

import java.util.UUID

import akka.actor.Actor
import akka.persistence.{PersistentImpl, CapabilityFlag}
import akka.persistence.JournalProtocol.{ReplayedMessage, RecoverySuccess, ReplayMessages}
import akka.persistence.couchbase.support.CouchbasePluginSpec
import akka.persistence.journal.JournalSpec
import akka.testkit.TestProbe

class CouchbaseJournalSpec
  extends JournalSpec(CouchbasePluginSpec.config)
  with CouchbasePluginSpec {

  override protected def supportsRejectingNonSerializableObjects: CapabilityFlag = CapabilityFlag.off

  "Couchbase journal" must {

    "replay all messages from corrupt store" in {

      val sender1 = TestProbe()
      val sender2 = TestProbe()

      val persistenceId = "corrupt"

      writeMessages(1, 5, persistenceId, sender1.ref, UUID.randomUUID.toString)
      writeMessages(1, 5, persistenceId, sender2.ref, UUID.randomUUID.toString)

      val receiverProbe = TestProbe()

      journal ! ReplayMessages(1, Long.MaxValue, Long.MaxValue, persistenceId, receiverProbe.ref)

      1 to 5 foreach { i =>
        receiverProbe.expectMsgType[ReplayedMessage].persistent.sequenceNr shouldBe i.toLong
        receiverProbe.expectMsgType[ReplayedMessage].persistent.sequenceNr shouldBe i.toLong
      }

      receiverProbe.expectMsg(RecoverySuccess(highestSequenceNr = 5L))
    }
  }
}