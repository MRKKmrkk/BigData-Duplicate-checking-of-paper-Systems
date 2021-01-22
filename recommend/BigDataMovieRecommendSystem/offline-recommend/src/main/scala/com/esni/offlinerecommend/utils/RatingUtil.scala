package com.esni.offlinerecommend.utils

import java.io.InputStream
import java.sql.{Connection, DriverManager}
import java.util.Properties

import com.esni.offlinerecommend.OfflineRecommender.{getConnection, sc}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.{JdbcRDD, RDD}

object RatingUtil {

  private val in: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("offline-recommend.properties")
  val properties = new Properties()
  properties.load(in)

  def getConnection(): Connection = {

    DriverManager.getConnection(properties.getProperty("database.mysql.uri"), properties.getProperty("database.mysql.user"), properties.getProperty("database.mysql.password"))

  }

  def getRatings(scoreLogFilePath: String, behaviorLogFilePath: String, sc: SparkContext): RDD[Rating] = {

    // 0	1984	5	1611039929077
    val scoresRdd = sc.textFile(scoreLogFilePath)
      .map(_.split("\t"))
      .map(x => ((x(0).toInt, x(1).toInt), (x(2).toDouble, x(3).toLong)))
      .groupByKey()
      .map{x =>
        val maxTup = x._2.toList.sortWith(_._2 > _._2).head
        (x._1._1, x._1._2, maxTup._1, maxTup._2)
      }

    // 971	3246	search	1611039930264
    val behaviosrRdd = sc.textFile(behaviorLogFilePath)
      .map(_.split("\t"))
      .map{x =>
        var score = 0.0
        if ("click".equals(x(2))) {
          score = 1.0
        }
        if ("search".equals(x(2))) {
          score = 2.0
        }
        if ("like".equals(x(2))) {
          score = 2.0
        }
        if ("unlike".equals(x(2))) {
          score = -2.0
        }
        ((x(0).toInt, x(1).toInt, score), x(3).toLong)
      }
      .groupByKey()
      .map(x => (x._1._1, x._1._2, x._1._3, x._2.toList.sortWith(_ > _).head))

    // 计算偏好值，封装为rating
    val ratings = scoresRdd.union(behaviosrRdd)
      .map(x => ((x._1, x._2), x._3))
      .reduceByKey(_+_)
      .map{case (a,b) =>
        Rating(a._1, a._2, b)
      }

    ratings

  }

  def getUserAndMovieId: RDD[(Int, Int)] = {

    new JdbcRDD(sc, getConnection, "select user_id from user_info where user_id <= ? and user_id >= ?", 100000, 0, 1, rs => {
      rs.getInt(1)
    } ).cartesian{
      new JdbcRDD(sc, getConnection, "select movie_id from movie_info where movie_id <= ? and movie_id >= ?", 100000, 0, 1, rs => {
        rs.getInt(1)
      })
    }

  }

}
