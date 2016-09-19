import org.apache.spark.streaming.scheduler._
import spray.json._

/**
  * Created by tenghuanhe on 2016/9/18.
  */

object CustomJsonProtocol extends DefaultJsonProtocol {

  implicit object BatchInfoJsonFormat extends RootJsonFormat[BatchInfo] {
    override def write(batchInfo: BatchInfo): JsValue =
      JsObject(
        "numRecords" -> JsNumber(batchInfo.numRecords),
        "batchTime" -> JsNumber(batchInfo.batchTime.milliseconds),
        "submissionTime" -> JsNumber(batchInfo.submissionTime),
        "processingStartTime" -> JsNumber(batchInfo.processingStartTime.get),
        "processingEndTime" -> JsNumber(batchInfo.processingEndTime.get),
        "processingDelay" -> JsNumber(batchInfo.processingDelay.get),
        "schedulingDelay" -> JsNumber(batchInfo.schedulingDelay.get),
        "totalDelay" -> JsNumber(batchInfo.totalDelay.get)
      )

    // TODO
    override def read(json: JsValue): BatchInfo = ???
  }

}

class CustomStreamingListener extends StreamingListener {
  /** Called when a receiver has been started */
  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
    // add receiver start time in receiverInfo
  }

  /** Called when a receiver has reported an error */
  override def onReceiverError(receiverError: StreamingListenerReceiverError) {}

  /** Called when a receiver has been stopped */
  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) {}

  /** Called when a batch of jobs has been submitted for processing. */
  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
  }

  /** Called when processing of a batch of jobs has started.  */
  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) {}

  /** Called when processing of a batch of jobs has completed. */
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    import CustomJsonProtocol._
    val batchInfo = batchCompleted.batchInfo
    val infoJson: JsValue = batchInfo.toJson
    println(infoJson.prettyPrint)
  }
}
