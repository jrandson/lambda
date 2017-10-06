package batch

import java.lang.management.ManagementFactory

import domain.Activity
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

object batchjob {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
        .setAppName("lambda with Spark")

    val sc = new SparkContext(conf)
    implicit  val sqlContext = new SQLContext(sc)

    import org.apache.spark.sql.functions._
    import sqlContext.implicits._

    val sourceFile = "file:///C:\\Users\\jose.randson.d.cunha\\pluralsight\\lambda-pluralsight\\output\\data.csv"
    val input = sc.textFile(sourceFile)

    val inputDF = input.flatMap{ line =>
      val record = line.split("\\t")
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
      inputDF("refeerer"), inputDF("action"), inputDF("prevPage"), inputDF("rpage"), inputDF("visitor"), inputDF("product")
    ).cache()

    df.registerTempTable("activity")

    val visitorsByProduct = sqlContext.sql(
      """
        |SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors
        |FROM activity GROUP BY product , timestamp_hour
      """.stripMargin)

    visitorsByProduct.printSchema()

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

    activityByProduct.write.partitionBy("timestamp_hour").mode(SaveMode.Append).parquet("hdfs:///user/pluralsight/output/")


    val underExposedProducts = sqlContext.sql(
      """
        |select product,
        |timestamp_hour,
        UnderExposed(page_view_count, purchase_count) as negative_exposure
        from activityByProduct
        order by  negative_exposure DESC
        limit 5
      """.stripMargin)



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
