import org.apache.spark.scheduler._

/**
  * Created by tenghuanhe on 2016/9/18.
  */
class CustomSparkListener extends SparkListener {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {}

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {}

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {}

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {}

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskId = taskEnd.taskInfo.taskId
    val jvmGCTime = taskEnd.taskMetrics.jvmGCTime
    val executorRunTime = taskEnd.taskMetrics.executorRunTime
    val executorDeserializeTime = taskEnd.taskMetrics.executorDeserializeTime
    println(s"taskId = $taskId, jvmGCTime = $jvmGCTime, executorRunTime = $executorRunTime, " +
      s"executorDeserializeTime = $executorDeserializeTime")
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
