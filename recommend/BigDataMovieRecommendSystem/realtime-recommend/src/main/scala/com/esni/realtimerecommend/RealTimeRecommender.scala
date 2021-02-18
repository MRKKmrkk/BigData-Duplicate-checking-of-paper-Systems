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

object RealTimeRecommender {

  // 配置spark环境
  val conf: SparkConf = new SparkConf().setAppName("RealTime Recommender Demo").setMaster("local[*]")
  val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  val sc: SparkContext = session.sparkContext
  val ssc = new StreamingContext(sc, Seconds(2))

  // 获取配置文件
  private val in: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("realtime-recommender.properties")
  val properties = new Properties()
  properties.load(in)

  // 配置redis
  private val jedis = new Jedis(properties.getProperty("redis.host"), properties.getProperty("redis.port").toInt)

  // 配置kafka信息
  private val kafkaParams = Map(
    "bootstrap.servers" -> properties.getProperty("bootstrap.servers"),
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> properties.getProperty("group.id"),
    "auto.offset.reset" -> properties.getProperty("auto.offset.reset")
  )
  val topics: Array[String] = properties.getProperty("topics").split(",")

  /**
    * 获取用户最近limit个评分电影
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

  /**
    * 获取某个电影的前limit个相似电影
    */
  def getTopSimMovie(movieId: Int, limit: Int): Array[Int] = {

    MysqlUtil
      .getMovieMatrixAsArray(movieId)
      .take(limit)
      .map(_._1)

  }

  /**
    * 从电影相似度矩阵中，获取投票电影与相似电影的相似度
    */
  def getMovieSim(scoreMovie: Int, simMovie: Int, simRecs: scala.collection.mutable.HashMap[Int, Map[Int, Double]]): Double = {

    simRecs.get(scoreMovie) match {
      case Some(simMovies) => {
        simMovies.get(simMovie) match {
          case Some(sim) => sim
          case None => 0.0
        }
      }
      case None => 0.0
    }

  }

  /**
    * 计算对数
    */
  def log(m: Int): Double = {

    if (m == 0) {
      return 0.0
    }

    math.log(m) / math.log(2)

  }

  /**
    * 计算电影推荐的优先级
    */
  def computePriority(recentScoreMovies: Array[(Int, Double)], simMovies: Array[Int], simRecs: scala.collection.mutable.HashMap[Int, Map[Int, Double]]): Array[(Int, Double)] = {

    // 存放电影与优先级
    val prioritys = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    //存放增强因子
    val in = scala.collection.mutable.HashMap[Int, Int]()
    // 存放减弱因子
    val de = scala.collection.mutable.HashMap[Int, Int]()

    for (top <- simMovies; recentScore <- recentScoreMovies) {
      print("==")
      // 计算此电影的相似电影与近期投票电影的相似度
      val sim = getMovieSim(top, recentScore._1, simRecs)

      // 相似度小于0.3的相似电影会被忽略
      if (sim > 0.3){
        // 计算相似电影的优先级优先级
        prioritys +=  ((top, sim * recentScore._2.formatted("%.2f").toDouble))
        // 判断近期电影的投票，大于三增加增强因子，小于三增加减弱因子
        if (recentScore._2 > 3){
          in(top) = in.getOrDefault(top, 0) + 1
          de(top) = de.getOrDefault(top, 0)
        }
        else{
          de(top) = de.getOrDefault(top, 0) + 1
          in(top) = in.getOrDefault(top, 0)
        }
      }

    }

    prioritys
      .groupBy(_._1)
      .map{case (movieId, priority) =>
        (
          movieId,
          priority.map(_._2).sum / priority.length
            + log(in(movieId))
            - log(de(movieId))
        )
      }
      .toArray

  }

  /**
    * 获取所有相似电影的相似度矩阵
    */
  def getSimRec(simMovies: Array[Int]): scala.collection.mutable.HashMap[Int, Map[Int, Double]] = {

    val simRecs = scala.collection.mutable.HashMap[Int, Map[Int, Double]]()

    simMovies
      .foreach{
        movieId =>
          simRecs(movieId) = MysqlUtil.getMovieMatrixAsMap(movieId)
      }

    simRecs

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
      val fields = message.value().split("\t")
      (fields(0).toInt, fields(1).toInt, fields(2).toDouble, fields(3).toLong)
    }

    scoreStream.foreachRDD{rdd =>
      rdd
        .map{ case (userId, movieId, score, timestamp) =>
          val recentScoreMovies = getUserRecentScore(userId, 10)
          val simMovies = getTopSimMovie(movieId, 10)
          val simRecs: scala.collection.mutable.HashMap[Int, Map[Int, Double]] = getSimRec(simMovies)

          val result: Array[(Int, Double)] = computePriority(recentScoreMovies, simMovies, simRecs: scala.collection.mutable.HashMap[Int, Map[Int, Double]])
          MysqlUtil.saveRealTimeMovieRecommend(userId, result)

        }.count()
    }

    ssc.start()
    ssc.awaitTermination()
    jedis.close()
    MysqlUtil.close()

  }

}
