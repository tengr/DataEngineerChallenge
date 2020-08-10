package com.tengr.paypay

/**
  * Created by ruichen on 9/8/20.
  */

import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{countDistinct, lag}

class CoreUnitTest extends AnyFunSuite with
  BeforeAndAfterEach with BeforeAndAfterAll{

  @transient var sc: SparkContext = null
  @transient var spark: SparkSession = null

  override def beforeAll(): Unit = {

    spark = SparkSession.builder.master("local").getOrCreate()
    sc = spark.sparkContext
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    sc.stop()
  }


  test("integration") {

    val getEpoch = udf((date: String) => {
      val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
      val dt = df.parse(date);
      val epoch = dt.getTime();
      (epoch)
    })

    val rdd = sc.parallelize(Seq(
      "2015-07-22T09:00:27.894580Z marketpalce-shop ip1",
      "2015-07-22T09:00:27.885745Z marketpalce-shop ip2",
      "2015-07-22T09:00:28.048369Z marketpalce-shop ip3",
      "2015-07-22T09:00:28.036251Z marketpalce-shop ip4",
      "2015-07-22T09:00:28.033793Z marketpalce-shop ip5",
      "2015-07-22T09:00:28.055029Z marketpalce-shop ip1",
      "2015-07-22T09:00:28.050298Z marketpalce-shop ip1",
      "2015-08-22T09:00:28.059081Z marketpalce-shop ip5",
      "2015-07-22T09:00:28.054939Z marketpalce-shop ip2",
      "2015-07-22T09:00:28.064841Z marketpalce-shop ip1"))
      .map(_.split("\\s+" ))
      .map(arr => (arr(0), arr(1), arr(2)))

    //assert total count
    assert(rdd.count == 10)


    //assert total count
    val df = spark.createDataFrame(rdd).toDF("date", "elb", "ip").withColumn("timestamp", getEpoch(col("date")))
    assert(df.count == 10)


    val windowSpec = Window.partitionBy("ip").orderBy("timestamp")
    val duration = df.withColumn(
      "duration", col("timestamp") - lag(df("timestamp"), 1).over(windowSpec))
      .na.fill(0, Seq("duration"))


    val sessionFlag = duration.withColumn("newSession", when(col("duration") > 15 * 60 * 1000, 1).otherwise(0))
    //assert ip1 has 1 session
    assert(sessionFlag.filter(sessionFlag("ip") === "ip1").agg(countDistinct(col("newSession"))).collect()(0)(0) == 1)
    //assert ip5 has 2 sessions
    assert(sessionFlag.filter(sessionFlag("ip") === "ip5").agg(countDistinct(col("newSession"))).collect()(0)(0) == 2)


    val session = sessionFlag.withColumn("sessionId",
      concat(col("ip"), sum("newSession").over(windowSpec).cast("string")))
    //assert total of 6 sessions
    assert(session.agg(countDistinct(col("sessionId"))).collect()(0)(0) == 6)

  }
}