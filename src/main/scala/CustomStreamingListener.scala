import org.apache.spark.streaming.scheduler._

/**
  * Created by tenghuanhe on 2016/9/18.
  */
class CustomStreamingListener extends StreamingListener {
  /** Called when a receiver has been started */
  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted) {}

  /** Called when a receiver has reported an error */
  override def onReceiverError(receiverError: StreamingListenerReceiverError) {}

  /** Called when a receiver has been stopped */
  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped) {}

  /** Called when a batch of jobs has been submitted for processing. */
  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) {}

  /** Called when processing of a batch of jobs has started.  */
  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) {}

  /** Called when processing of a batch of jobs has completed. */
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    println("Batch completed, Total delay :" + batchCompleted.batchInfo.totalDelay.get.toString + " ms")
    println(batchCompleted.batchInfo)
    println(batchCompleted.batchInfo.streamIdToInputInfo)
  }
}
