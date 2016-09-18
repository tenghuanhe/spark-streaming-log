import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by tenghuanhe on 2016/9/18.
  */
object SparkVisual {
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName("SparkVisual").setMaster("local[4]")
    // Create the context
    val sc = new SparkContext(sparkConf)
    sc.addSparkListener(new CustomSparkListener())
    val rdd1 = sc.parallelize(1 to 100, 10)
    val rdd2 = rdd1.filter(x => x % 7 != 0).map(x => (x % 10, 1)).reduceByKey(_ + _)
    rdd2.saveAsTextFile("rdd2.txt")

    Thread.sleep(1000000)

  }
}
