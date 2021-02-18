package com.esni.realtimerecommend

import java.io.InputStream
import java.util.Properties

import com.esni.realtimerecommend.util.MysqlUtil
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._

object RealTimeRecommenderDemo {

  val conf: SparkConf = new SparkConf().setAppName("RealTime Recommender Demo").setMaster("local[*]")
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
      }
      .toArray
      .take(10)

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
  def getTopSimMovie(movieId: Int, limit: Int): Array[(Int, Double)] = {

    MysqlUtil
      .getMovieMatrixAsArray(movieId)
      .take(limit)

  }

  /**
    * 从电影相似度矩阵中，获取投票电影与相似电影的相似度
    */
  def getMovieSim(scoreMovie: Int, simMvie: Int): Double = {

//    val movieSimMatrix =broad.value

    val movieSimMatrix = MysqlUtil.getMovieMatrixAsMap(scoreMovie)
    if (movieSimMatrix.nonEmpty){
      movieSimMatrix.get(simMvie) match {
        case Some(sim) => sim
        case None => 0.0
      }
    }
    else{
      0.0
    }

//    movieSimMatrix.get(scoreMovie) match {
//      case Some(simMap) => simMap.get(simMvie) match {
//        case Some(sim) => sim
//        case None => 0.0
//      }
//      case None => 0.0
//    }

  }

  def log(m: Int): Double = {

    if (m == 0) {
      return 0.0
    }

    math.log(m) / math.log(2)

  }

  /**
    * 计算电影推荐的优先级
    */
  def computePriority(recentScoreMovies: Array[(Int, Double)], simMovies: Array[(Int, Double)]): Array[(Int, Double)] = {

    val moviePrioritys = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    for (simMovie <- simMovies; scoreMovie <- recentScoreMovies){
      print("**")
      val sim = getMovieSim(scoreMovie._1, simMovie._1)
      if (sim > 0.6){
        moviePrioritys += ((simMovie._1, sim * scoreMovie._2))
        if (scoreMovie._2 > 3){
          increMap(simMovie._1) = increMap.getOrDefault(simMovie, 0) + 1
        }
        else{
          decreMap(simMovie._1) = decreMap.getOrDefault(simMovie, 0) + 1
        }
      }
    }

    moviePrioritys
      .groupBy(_._1)
      .map{case (movieId, priority) =>
        (movieId, priority.map(_._2).sum / priority.length + log(increMap(movieId)) - log(decreMap(movieId)))
      }.toArray

  }

  def testComputePriority(recentScoreMovies: Array[(Int, Double)], simMovies: Array[Int]): Array[(Int, Double)] = {

    val moviePrioritys = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    ////////////////////////// 此方法 未经验证 /////////////////////////
    for (simMovie <- simMovies; scoreMovie <- recentScoreMovies){
      print("**")
      val sim = getMovieSim(simMovie, scoreMovie._1)
      if (sim > 0.6){
        moviePrioritys += ((simMovie, sim * scoreMovie._2))
        if (scoreMovie._2 > 3){
          increMap(simMovie) = increMap.getOrDefault(simMovie, 0) + 1
          decreMap(simMovie) = decreMap.getOrElseUpdate(simMovie, 0)
        }
        else{
          decreMap(simMovie) = decreMap.getOrDefault(simMovie, 0) + 1
          increMap(simMovie) = increMap.getOrDefault(simMovie, 0)
        }
      }
    }
    ////////////////////////// 此方法 未经验证 /////////////////////////

    moviePrioritys
      .groupBy(_._1)
      .map{case (movieId, priority) =>
        (
          movieId,
          priority.map(_._2).sum / priority.length
            + log(increMap(movieId))
            - log(decreMap(movieId))
        )
      }.toArray

  }

  def main(args: Array[String]): Unit = {

    val kafkaDataStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](
        topics,kafkaParams
      )
    )
    val scoreStream = kafkaDataStream.map{message =>
      print("***" + message.value())
      val fields = message.value().split("\t")
      (fields(0).toInt, fields(1).toInt, fields(2).toDouble, fields(3).toLong)
    }

    scoreStream.foreachRDD{rdd =>
      rdd
        .map{ case (userId, movieId, score, timestamp) =>
            val recentScoreMovies = getUserRecentScore(userId, 10)
            print("***1")
            val simMovies = getTopSimMovie(movieId, 10)
            print("***2")
            val result: Array[(Int, Double)] = computePriority(recentScoreMovies, simMovies)
            print("***3")
            MysqlUtil.saveRealTimeMovieRecommend(userId, result)
            print("***4")
        }.count()
    }

    ssc.start()
    ssc.awaitTermination()
    MysqlUtil.close()

  }

}


