package batch

//import java.lang.management.ManagementFactory

import java.lang.management.ManagementFactory

import domain._
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql.{SQLContext, SaveMode}
import utils.SparkUtils._



object BatchJob {
  def main(args: Array[String]): Unit = {

    var checkPointDirectory = ""


    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val sourceFile = "file://C:\\Users\\jose.randson.d.cunha\\VirtualBox VMs\\spark-kafka-cassandra-applying-lambda-architecture\\vagrant\\datasource\\data.csv"
    val input = sc.textFile(sourceFile)

   val inputDF = input.flatMap{ line =>
     val record = line.split(",")
     val MS_IN_HOUR = 1000 * 60 * 60
     if (record.length == 7) {
       Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
     }
     else
       None
   }.toDF()

   sqlContext.udf.register("UnderExposed", (pageViewCount: Long, purcaheCount: Long) => if (purcaheCount == 0) 0 else pageViewCount / purcaheCount)


   val df = inputDF.select(
     //add_months(inputDF("timestamp_hour"),1).as("timestamp_hour"),
     add_months(from_unixtime(inputDF("timestamp_hour")/1000), 1).as("timestamp_hour"),
     inputDF("referrer"), inputDF("action"), inputDF("prevPage"), inputDF("page"), inputDF("visitor"), inputDF("product")
   ).cache()

   df.registerTempTable("activity")

   val visitorsByProduct = sqlContext.sql(
     """
       |SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors
       |FROM activity GROUP BY product , timestamp_hour
     """.stripMargin)

  visitorsByProduct.printSchema()
    visitorsByProduct.collect()

   /*
  val keyedByProduct = inputRDD.keyBy(a => (a.product, a.timestamp_hour)).cache()
  val  visitorsByProduct = keyedByProduct
    .mapValues(a => a.visitor)
    .distinct()
    .countByKey()
  */


  val activityByProduct = sqlContext.sql("""SELECT
                                         product,
                                         timestamp_hour,
                                         sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
                                         sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
                                         sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
                                         from activity
                                         group by product, timestamp_hour """).cache()

    /*
    activityByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs://lambda-pluralsight:9000/user/pluralsight/output/")



   val underExposedProducts = sqlContext.sql(
     """
       |select product,
       |timestamp_hour,
       UnderExposed(page_view_count, purchase_count) as negative_exposure
       from activityByProduct
       order by  negative_exposure DESC
       limit 5
     """.stripMargin)
    */


   /*
   val activityByProduct = keyedByProduct
     .mapValues { a =>
       a.action match {
         case "purchase" => (1, 0, 0)
         case "add_to_cart" => (0, 1, 0)
         case "page_view" => (0, 0, 1)
       }
     }
     .reduceByKey((a,b) => (a._1 + b._1,  a._2 + b._2, a._3 + b._3))
   */

 visitorsByProduct.foreach(println)
 activityByProduct.foreach(println)


  }
}
