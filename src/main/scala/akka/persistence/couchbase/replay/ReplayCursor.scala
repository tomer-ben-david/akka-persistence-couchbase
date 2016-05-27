package akka.persistence.couchbase.replay

case class ReplayCursor private(journalMessageId: Long,
                                sequenceNrOption: Option[Long],
                                docIdOption: Option[String]) {

  def update(journalMessageId: Long, sequenceNr: Long, docId: String) = {
    copy(
      journalMessageId = journalMessageId,
      sequenceNrOption = Some(sequenceNr),
      docIdOption = Some(docId)
    )
  }
}

object ReplayCursor {

  def apply(journalMessageId: Long): ReplayCursor = {
    ReplayCursor(journalMessageId, None, None)
  }
}
