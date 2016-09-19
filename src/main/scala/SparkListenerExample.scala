import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by tenghuanhe on 2016/9/18.
  */
object SparkListenerExample {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName("SparkListenerExample").setMaster("local[4]")
    // Create the context
    val sc = new SparkContext(sparkConf)
    sc.addSparkListener(new CustomSparkListener())
    val rdd1 = sc.textFile("cluster_map.txt").map {
      case line =>
        val tuple = line.split("\t")
        (Integer.parseInt(tuple(1)), tuple(0))
    }
    val rdd2 = rdd1.map(x => (x._1 * 13 + 7, x._2)).filter(x => x._1 % 7 != 0).groupBy(x => x._1)
    rdd2.saveAsTextFile("rdd2.txt")

    Thread.sleep(1000000)

  }
}
