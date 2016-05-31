package akka.persistence.couchbase.replay

case class ReplayCursor private(journalMessageIdOption: Option[Long],
                                sequenceNrOption: Option[Long],
                                docIdOption: Option[String]) {

  def update(journalMessageId: Long, sequenceNr: Long, docId: String) = {
    copy(
      journalMessageIdOption = Some(journalMessageId),
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
