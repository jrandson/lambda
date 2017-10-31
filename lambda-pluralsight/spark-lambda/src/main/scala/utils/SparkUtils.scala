package utils

import java.lang.management.ManagementFactory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
import utils.SparkUtils._

import scala.concurrent.duration.Duration

object SparkUtils {
  def getSparkContext(appName: String) = {

    val isIDE = {
      ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("Intellij IDEA")
    }

    var checkpointDirectory = ""

    val conf = new SparkConf().setAppName(appName).setMaster("local[2]").set("spark.executor.memory", "1g")
    //val conf = new SparkConf()
    //    .setAppName("lambda with Spark")

    System.setProperty("hadoop.home.dir", "C:\\Libraries\\WinUtils")

    if (isIDE){
      System.setProperty("hadoop.home.dir", "C:\\Libraries\\WinUtils")
      conf.setMaster("local[*]")
      checkpointDirectory = "file:///C:\\Users\\jose.randson.d.cunha\\Documents\\pluralsight\\data\\checkpoint"
    }
    else{
      checkpointDirectory = "hdfs://127.0.0.1:9000/spark/checkpoint"
    }

    checkpointDirectory = "file:///C:\\Users\\jose.randson.d.cunha\\Documents\\pluralsight\\data\\checkpoint"

    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkpointDirectory)
    sc
  }

  def getSQLContext(sc: SparkContext) ={
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext
  }

  def getStreamingContext(streamingApps: (SparkContext, Duration) => StreamingContext, sc: SparkContext, batchDuration: Duration) = {
    val creatingFunc = () => streamingApps(sc, batchDuration)
    val ssc = sc.getCheckpointDir match {
      case Some(checkPointDir) => StreamingContext.getActiveOrCreate(checkPointDir, creatingFunc, sc.hadoopConfiguration, createOnError = true)
      case None => StreamingContext.getActiveOrCreate(creatingFunc)
    }
    sc.getCheckpointDir.foreach(cp => ssc.checkPoint(cp))
    ssc
  }
}
