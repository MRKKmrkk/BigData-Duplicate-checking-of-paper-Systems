package com.esni.realtimerecommend

import java.io.InputStream
import java.util.Properties

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RealTimeRecommender {

  val conf: SparkConf = new SparkConf().setAppName("RealTime Recommender").setMaster("local[*]")
  val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  val sc: SparkContext = session.sparkContext
  val ssc = new StreamingContext(sc, Seconds(2))

  private val in: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("stream.properties")
  val properties = new Properties()
  properties.load(in)

  val kafkaParams = Map(
    "bootstrap.servers" -> properties.getProperty("bootstrap.servers"),
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> properties.getProperty("group.id"),
    "auto.offset.reset" -> properties.getProperty("auto.offset.reset")
  )
  val topics: Array[String] = properties.getProperty("topics").split(",")

  def main(args: Array[String]): Unit = {

    val kafkaDataStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        topics,kafkaParams
      )
    )
    val scoreStream = kafkaDataStream.map{message =>
      val fields = message.value().split("\t")
      (fields(0).toInt, fields(1).toInt, fields(2).toDouble, fields(3).toLong)
    }

    scoreStream.foreachRDD{rdd =>
      rdd.foreach(println)
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
