import org.apache.spark.scheduler._
import spray.json._

/**
  * Created by tenghuanhe on 2016/9/18.
  */

case class CustomTaskInfo(taskId: Long,
                          launchTime: Long)

case class CustomTaskType(taskType: String)

case class CustomInputMetrics(bytesRead: Long,
                              recordsRead: Long)

case class CustomOutputMetrics(bytesWritten: Long,
                               recordsWritten: Long)

case class CustomShuffleReadMetrics(localBlocksFetched: Int,
                                    remoteBlocksFetched: Int,
                                    totalBlocksFetched: Int,
                                    localBytesRead: Long,
                                    remoteBytesRead: Long,
                                    totalBytesRead: Long,
                                    recordsRead: Long,
                                    fetchWaitTime: Long)

case class CustomShuffleWriteMetrics(shuffleBytesWritten: Long,
                                     shuffleRecordsWritten: Long,
                                     shuffleWriteTime: Long)

case class CustomTaskMetrics(jvmGCTime: Long,
                             executorRunTime: Long,
                             executorDeserializeTime: Long,
                             inputMetrics: CustomInputMetrics,
                             outputMetrics: CustomOutputMetrics,
                             shuffleReadMetrics: CustomShuffleReadMetrics,
                             shuffleWriteMetrics: CustomShuffleWriteMetrics)

case class CustomTaskInfoAndMetrics(stageId: Int,
                                    stageAttemptId: Int,
                                    taskType: CustomTaskType,
                                    taskInfo: CustomTaskInfo,
                                    taskMetrics: CustomTaskMetrics)

trait CustomClassSprayProtocol extends DefaultJsonProtocol {
  implicit val customTaskInfoClassFormat = jsonFormat2(CustomTaskInfo)
  implicit val customTaskTypeClassFormat = jsonFormat1(CustomTaskType)
  implicit val customInputMetricsClassFormat = jsonFormat2(CustomInputMetrics)
  implicit val customOutputMetricsClassFormat = jsonFormat2(CustomOutputMetrics)
  implicit val customShuffleReadMetrics = jsonFormat8(CustomShuffleReadMetrics)
  implicit val customShuffleWriteMetrics = jsonFormat3(CustomShuffleWriteMetrics)
  implicit val customTaskMetricsClassFormat = jsonFormat7(CustomTaskMetrics)
  implicit val customTaskInfoAndMetricsClassFormat = jsonFormat5(CustomTaskInfoAndMetrics)
}

object CustomClassSprayProtocol extends CustomClassSprayProtocol

class CustomSparkListener extends SparkListener {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {}

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {}

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {}

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {

    import CustomClassSprayProtocol._
    // this line is necessary

    // get taskId
    val stageId = taskEnd.stageId
    val stageAttemptId = taskEnd.stageAttemptId

    val taskType = taskEnd.taskType
    val customTaskType: CustomTaskType = CustomTaskType(taskType)

    val taskInfo = taskEnd.taskInfo
    val taskId = taskInfo.taskId
    val launchTime = taskInfo.launchTime
    val customTaskInfo: CustomTaskInfo = CustomTaskInfo(taskId, launchTime)

    val taskMetrics = taskEnd.taskMetrics
    // get time metrics
    val jvmGCTime = taskMetrics.jvmGCTime
    val executorRunTime = taskMetrics.executorRunTime
    val executorDeserializeTime = taskMetrics.executorDeserializeTime


    val inputMetrics = taskMetrics.inputMetrics
    val outputMetrics = taskMetrics.outputMetrics
    val shuffleReadMetrics = taskMetrics.shuffleReadMetrics
    val shuffleWriteMetrics = taskEnd.taskMetrics.shuffleWriteMetrics

    val customInputMetrics = inputMetrics.isDefined match {
      case true =>
        CustomInputMetrics(inputMetrics.get.bytesRead, inputMetrics.get.recordsRead)
      case false =>
        CustomInputMetrics(-1, -1)
    }

    val customOutputMetrics = outputMetrics.isDefined match {
      case true =>
        CustomOutputMetrics(outputMetrics.get.bytesWritten, outputMetrics.get.recordsWritten)
      case false =>
        CustomOutputMetrics(-1, -1)
    }

    val customShuffleReadMetrics = shuffleReadMetrics.isDefined match {
      case true =>
        CustomShuffleReadMetrics(shuffleReadMetrics.get.localBlocksFetched, shuffleReadMetrics.get.remoteBlocksFetched,
          shuffleReadMetrics.get.totalBlocksFetched, shuffleReadMetrics.get.localBytesRead,
          shuffleReadMetrics.get.remoteBytesRead, shuffleReadMetrics.get.totalBytesRead,
          shuffleReadMetrics.get.recordsRead, shuffleReadMetrics.get.fetchWaitTime)
      case false =>
        CustomShuffleReadMetrics(-1, -1, -1, -1, -1, -1, -1, -1)
    }

    val customShuffleWriteMetrics = shuffleWriteMetrics.isDefined match {
      case true =>
        CustomShuffleWriteMetrics(shuffleWriteMetrics.get.shuffleBytesWritten,
          shuffleWriteMetrics.get.shuffleRecordsWritten, shuffleWriteMetrics.get.shuffleWriteTime)
      case false =>
        CustomShuffleWriteMetrics(-1, -1, -1)
    }

    val customTaskMetrics = CustomTaskMetrics(jvmGCTime, executorRunTime, executorDeserializeTime, customInputMetrics,
      customOutputMetrics, customShuffleReadMetrics, customShuffleWriteMetrics)
    val customTaskInfoAndMetrics = CustomTaskInfoAndMetrics(stageId, stageAttemptId, customTaskType, customTaskInfo,
      customTaskMetrics)
    val infoJson: JsValue = customTaskInfoAndMetrics.toJson
    println(infoJson.prettyPrint)
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {}

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {}

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {}

  override def onBlockManagerRemoved(
                                      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {}

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {}

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {}

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {}

  override def onExecutorMetricsUpdate(
                                        executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {}

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {}

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {}

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {}

}
