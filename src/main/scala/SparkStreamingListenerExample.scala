import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable
import scala.collection.mutable.Queue

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by tenghuanhe on 2016/9/16.
  */
object SparkStreamingListenerExample {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName("SparkStreamingListenerExample").setMaster("local[1]")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Create the queue through which RDDS can be pushed to
    // a QueueInputDStream
    val rddQueue = new mutable.Queue[RDD[Int]]()

    // Create the QueueInputDStream use it do some processing
    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    //    val reducedStream = mappedStream.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(3), Seconds(1))
    reducedStream.print()
    ssc.addStreamingListener(new CustomStreamingListener())
    ssc.sparkContext.addSparkListener(new CustomSparkListener())
    ssc.start()

    // Create and push some RDDs into rddQueue
    for (i <- 1 to 5) {
      rddQueue.synchronized {
        rddQueue += ssc.sparkContext.makeRDD(1 to 10000, 1)
      }

      Thread.sleep(1000)
    }

    ssc.stop()

  }
}
