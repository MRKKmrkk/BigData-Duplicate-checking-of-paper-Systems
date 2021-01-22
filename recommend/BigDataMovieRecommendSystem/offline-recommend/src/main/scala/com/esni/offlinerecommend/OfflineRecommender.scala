package com.esni.offlinerecommend

import java.io.InputStream
import java.util.Properties

import com.esni.offlinerecommend.als.ALSTrainer
import com.esni.offlinerecommend.utils.RatingUtil
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

case class OfflineUserRecommend(user_id: Int, movie_1: Int, movie_2: Int, movie_3: Int, movie_4: Int, movie_5: Int, movie_6: Int, movie_7: Int, movie_8: Int, movie_9: Int, movie_10: Int)

object OfflineRecommender {

  val conf: SparkConf = new SparkConf().setAppName("offline recommender").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  private val in: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("offline-recommend.properties")
  val properties = new Properties()
  properties.load(in)

  def saveInMysql(df: DataFrame, tableName: String, saveMode: SaveMode=SaveMode.Overwrite): Unit = {

    df.write
      .mode(saveMode)
      .jdbc(properties.getProperty("uri"), tableName, properties)

  }

  /**
    * 基于用户的电影推荐
    * 将推荐值前十的电影存入离线用户推荐电影表
    */
  def recommendDependOnUser(ratings: RDD[Rating], userAndMovieId: RDD[(Int, Int)]): Unit = {

    val trainer = new ALSTrainer(ratings)

    import session.implicits._
    val result = trainer.predict(userAndMovieId)
      .map(x => (x.user, (x.product, x.rating)))
      .groupByKey()
      .map(x => (x._1, x._2.toList.sortWith(_._2 > _._2).take(10)))
      .map{case (k, l) => OfflineUserRecommend(k, l(0)._1, l(1)._1, l(2)._1, l(3)._1, l(4)._1, l(5)._1, l(6)._1, l(7)._1, l(8)._1, l(9)._1)}
      .toDF()

    saveInMysql(result, "offline_user_recommend")


  }

  def main(args: Array[String]): Unit = {

    val ratings = RatingUtil.getRatings("D:\\Projects\\dpystem\\recommend\\testData\\ur.log",
      "D:\\Projects\\dpystem\\recommend\\testData\\ub.log", sc)

    val userAndMovieId = RatingUtil.getUserAndMovieId(sc)

    recommendDependOnUser(ratings, userAndMovieId)

    }

}
