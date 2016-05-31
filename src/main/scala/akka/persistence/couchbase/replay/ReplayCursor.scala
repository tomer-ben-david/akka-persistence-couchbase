package akka.persistence.couchbase.replay

case class ReplayCursor private(journalMessageId: Option[Long],
                                sequenceNrOption: Option[Long],
                                docIdOption: Option[String]) {

  def update(journalMessageId: Long, sequenceNr: Long, docId: String) = {
    copy(
      journalMessageId = Some(journalMessageId),
      sequenceNrOption = Some(sequenceNr),
      docIdOption = Some(docId)
    )
  }
}

object ReplayCursor {

  def apply(journalMessageIdOption: Option[Long]): ReplayCursor = {
    ReplayCursor(journalMessageIdOption, None, None)
  }
}
