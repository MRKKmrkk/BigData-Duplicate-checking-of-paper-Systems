package com.esni.realtimerecommend

import java.io.InputStream
import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._

object ConnectionHelper{

  def getConnection() : Connection = {

    DriverManager.getConnection("jdbc:mysql://localhost:3306/train", "root", "mailbox330.")

  }

}

object RealTimeRecommender {

  val conf: SparkConf = new SparkConf().setAppName("RealTime Recommender").setMaster("local[*]")
  val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  val sc: SparkContext = session.sparkContext
  val ssc = new StreamingContext(sc, Seconds(2))

  private val in: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("realtime-recommender.properties")
  val properties = new Properties()
  properties.load(in)

  private val jedis = new Jedis(properties.getProperty("redis.host"), properties.getProperty("redis.port").toInt)

  private val kafkaParams = Map(
    "bootstrap.servers" -> properties.getProperty("bootstrap.servers"),
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> properties.getProperty("group.id"),
    "auto.offset.reset" -> properties.getProperty("auto.offset.reset")
  )
  val topics: Array[String] = properties.getProperty("topics").split(",")

  /////////////////////////算法开始实现//////////////////////////////////////

  /**
    * 获取用户最近评分电影
    */
  def getUserRecentScore(userId: Int, limit: Int): Array[(Int, Double)] = {

    jedis
      .lrange("user_id:" + userId, 0, limit)
      .map{line =>
        val fields = line.split(":")
        (fields(0).trim.toInt, fields(1).trim.toDouble)
      }.toArray

  }

//  /**
//    * 获取mysql连接对象
//    */
//  def getConnection(): Connection = {
//
//    DriverManager.getConnection(properties.getProperty("uri"), properties.getProperty("user"), properties.getProperty("password"))
//
//  }

  /**
    * 获取某个电影的前limit个相似电影
    */
  def getTopSimMovie(movieId: Int, limit: Int, broad: Broadcast[collection.Map[Int, Map[Int, Double]]]): Array[(Int, Double)] = {

    val movieSimMatrix =broad.value
    movieSimMatrix(movieId).toArray.take(limit)

  }

  /**
    * 从电影相似度矩阵中，获取投票电影与相似电影的相似度
    */
  def getMovieSim(scoreMovie: Int, simMvie: Int, broad: Broadcast[collection.Map[Int, Map[Int, Double]]]): Double = {

    val movieSimMatrix =broad.value

    movieSimMatrix.get(scoreMovie) match {
      case Some(simMap) => simMap.get(simMvie) match {
        case Some(sim) => sim
        case None => 0.0
      }
      case None => 0.0
    }

  }

  def log(m: Int): Double = {
    math.log(m) / math.log(2)
  }

  /**
    * 计算电影推荐的优先级
    */
  def computePriority(recentScoreMovies: Array[(Int, Double)], simMovies: Array[(Int, Double)], broad: Broadcast[collection.Map[Int, Map[Int, Double]]]): Array[(Int, Double)] = {

    val moviePrioritys = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    for (simMovie <- simMovies; scoreMovie <- recentScoreMovies){
      val sim = getMovieSim(scoreMovie._1, simMovie._1, broad)
      if (sim > 0.6){
        moviePrioritys += ((simMovie._1, sim * scoreMovie._2))
        if (scoreMovie._2 > 3){
          increMap(simMovie._1) = increMap.getOrDefault(simMovie._1, 0) + 1
        }
        else{
          decreMap(simMovie._1) = decreMap.getOrDefault(simMovie._1, 0) + 1
        }
      }
    }

    moviePrioritys
      .groupBy(_._1)
      .map{case (movieId, priority) =>
        (movieId, priority.map(_._2).sum / priority.length + log(increMap(movieId)) - log(decreMap(movieId)))
      }.toArray

  }

  def main(args: Array[String]): Unit = {

    //获取电影相似度矩阵,添加到广播变量
    val movieSimMatrix: collection.Map[Int, Map[Int, Double]] = new JdbcRDD(sc, ConnectionHelper.getConnection, "select movie_id,movie_matrix from movie_sim_matrix where movie_id <= ? and movie_id >= ?", 1000000, 0, 2, rs => {
      (rs.getInt(1), rs.getString(2))
    })
      .map { case (userId, simMatrix) =>
        val smMap = simMatrix.split(" ")
          .map(_.split(":"))
          .map(x => (x(0).toInt, x(1).toDouble))
          .toMap
        (userId, smMap)
      }
      .collectAsMap()
    val broad: Broadcast[collection.Map[Int, Map[Int, Double]]] = sc.broadcast(movieSimMatrix)

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
      rdd
        .map{ case (userId, movieId, score, timestamp) =>
            val recentScoreMovies = getUserRecentScore(userId, 10)
            val simMovies = getTopSimMovie(movieId, 10, broad)
            val result: Array[(Int, Double)] = computePriority(recentScoreMovies, simMovies, broad)
            result
        }.foreach(println)
    }

    ssc.start()
    ssc.awaitTermination()

  }

}
