package com.tengr.paypay
import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lag, countDistinct}

/**
  * Created by ruichen on 8/8/20.
  */
object Paypay {
  val PATTERN = """^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "(\S+ \S+ \S+)" "([^"]*)" (\S+) (\S+)""".r

  // timestamp elb client:port backend:port request_processing_time backend_processing_time response_processing_time elb_status_code backend_status_code received_bytes sent_bytes "request" "user_agent" ssl_cipher ssl_protocol
  object ColumnName extends Enumeration {
    val timestamp, elb, clientIp, backendIp, request_processing_time, backend_processing_time,
    response_processing_time, elb_status_code, backend_status_code, received_bytes, sent_bytes,
    request, user_agent, ssl_cipher, ssl_protocol = Value
  }

  def lineParser(line: String): List[String] = {
    try {
      val matchResult = PATTERN.findFirstMatchIn(line)
      val m = matchResult.get
      ColumnName.values.toList.map(colName => m.group(colName.id + 1))
    }
    catch {
      case e : Throwable => null
    }
  }

  def getEpoch(timestamp:String)= {
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    val dt = df.parse(timestamp);
    val epoch = dt.getTime();
    (epoch)
  }
  def main(args: Array[String]) = {

    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate()
    val sc = spark.sparkContext

    spark.sparkContext.setLogLevel("ERROR")

    // pre-process data
    val data = sc
      .textFile("data/2015_07_22_mktplace_shop_web_log_sample.log.gz")
      .map(lineParser)
      .filter(_ != null)
      .map(cols => (getEpoch(cols(ColumnName.timestamp.id)), cols(ColumnName.clientIp.id), cols(ColumnName.request.id), cols(ColumnName.user_agent.id)))

    println(data.count)

    // make dataframe
    val df = spark.createDataFrame(data).toDF(ColumnName.timestamp.toString, ColumnName.clientIp.toString, ColumnName.request.toString, ColumnName.user_agent.toString)

    // make window function over client with same ip   
    val windowSpec = Window.partitionBy(ColumnName.clientIp.toString).orderBy(ColumnName.timestamp.toString)

    val durationColName = "duration(ms)"
    val sessionIdColName = "sessionId"
    val newSessionColName = "newSession"

    // after sorting timestamps, take the difference with the previous timestamp
    val duration = df.withColumn(
      durationColName, col(ColumnName.timestamp.toString) - lag(df(ColumnName.timestamp.toString), 1).over(windowSpec))
      .na.fill(0, Seq(durationColName))

    // a new session is when the difference with the previous timestamp exceeds 15 min
    val sessionFlag = duration.withColumn(newSessionColName, when(col(durationColName) > 15 * 60 * 1000, 1).otherwise(0))

    // create "session id" based on ip, userAgent and newSession - just for demo purpose. This is obviously not the real-world session id
    val session = sessionFlag.withColumn(sessionIdColName,
      concat(col(ColumnName.clientIp.toString), sum(newSessionColName).over(windowSpec).cast("string"), col(ColumnName.user_agent.toString))).cache

    println("\n===================part1 : Sessionize the web log by IP ======================")
    session.show()
    session.repartition(1).write.format("csv").option("header",true).mode("overwrite").option("sep",",").save("result/part1_session.csv")

    println("\n===================part2 : Determine the average session time ======================")
    val part2 = session.select(mean(durationColName))
    part2.show()
    part2.repartition(1).write.format("csv").option("header",true).mode("overwrite").option("sep",",").save("result/part2_mean.csv")

    println("\n===================part3 : Determine unique URL visits per session ======================")
    val part3 = session.select(col(sessionIdColName),col(ColumnName.request.toString)).groupBy(sessionIdColName).agg(countDistinct(col(ColumnName.request.toString)))
    part3.show()
    part3.repartition(1).write.format("csv").option("header",true).mode("overwrite").option("sep",",").save("result/part3_unique_url_visits.csv")

    println("\n===================part4 : Find the most engaged users ======================")
    val part4 = session.groupBy(ColumnName.clientIp.toString).sum(durationColName).sort(col("sum(duration(ms))").desc)
    part4.show()
    part4.repartition(1).write.format("csv").option("header",true).mode("overwrite").option("sep",",").save("result/part4_most_engaged_user.csv")

    sc.stop()

  }
}
