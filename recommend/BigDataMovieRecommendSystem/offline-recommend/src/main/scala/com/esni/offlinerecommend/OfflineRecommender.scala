package com.esni.offlinerecommend

import java.io.InputStream
import java.util.Properties

import com.esni.offlinerecommend.als.ALSTrainer
import com.esni.offlinerecommend.bean.{MovieSimMatrix, OfflineUserRecommend}
import com.esni.offlinerecommend.utils.RatingUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix

object OfflineRecommender {

  val conf: SparkConf = new SparkConf().setAppName("offline recommender").setMaster("local[*]")
  val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  val sc: SparkContext = session.sparkContext

  private val in: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("offline-recommend.properties")
  val properties = new Properties()
  properties.load(in)

  def saveInMysql(df: DataFrame, tableName: String, saveMode: SaveMode=SaveMode.Overwrite): Unit = {

    df.write
      .mode(saveMode)
      .jdbc(properties.getProperty("uri"), tableName, properties)

  }

  /**
    * 计算两个矩阵之间的余弦相似度
    */
  def consineSim(dm1: DoubleMatrix, dm2: DoubleMatrix): Double = {

    dm1.dot(dm2) / (dm1.norm2() * dm2.norm2())

  }

  /**
    * 基于用户的电影推荐
    * 将推荐值前十的电影存入离线用户推荐电影表
    */
  def recommendDependOnUser(trainer: ALSTrainer, userAndMovieId: RDD[(Int, Int)]): Unit = {

    import session.implicits._
    val result = trainer.predict(userAndMovieId)
      .map(x => (x.user, (x.product, x.rating)))
      .groupByKey()
      .map(x => (x._1, x._2.toList.sortWith(_._2 > _._2).take(10)))
      .map{case (k, l) => OfflineUserRecommend(k, l(0)._1, l(1)._1, l(2)._1, l(3)._1, l(4)._1, l(5)._1, l(6)._1, l(7)._1, l(8)._1, l(9)._1)}
      .toDF()

    saveInMysql(result, "offline_user_recommend")

  }

  /**
    * 基于电影的推荐
    * 将相似度前十的电影存入离线电影相似性表中
    */
  def recommendDependOnMovie(trainer: ALSTrainer): Unit = {

    val moviesFeature = trainer
      .model
      .productFeatures
      .map{
        case (movieId, features) =>
          (movieId, new DoubleMatrix(features))
      }

    val carMovieFeature = moviesFeature
      .cartesian(moviesFeature)
      .filter{case (mf1, mf2) => mf1._1 != mf2._1}

    import session.implicits._
    val result = carMovieFeature
      .map{case (mf1, mf2) => (mf1._1, (mf2._1, consineSim(mf1._2, mf2._2)))}
      .filter(_._2._2 > properties.getProperty("sim.filter.threshold").toDouble)
      .groupByKey()
      .map(item => (item._1, item._2.toList.sortWith(_._2 > _._2)))
      .map{case (k, l) =>
        var line = ""
        for (movieSimTup <- l) {
          line += movieSimTup._1 + ":" + movieSimTup._2.formatted("%.2f") + " "
        }
        MovieSimMatrix(k, line)
      }
      .toDF()

    saveInMysql(result, "movie_sim_matrix")

  }

  def main(args: Array[String]): Unit = {

    val ratings = RatingUtil.getRatings("D:\\Projects\\dpystem\\recommend\\testData2.0\\ur",
      "D:\\Projects\\dpystem\\recommend\\testData2.0\\ub", sc)
    val trainer = new ALSTrainer(ratings)
    trainer.trainModel(90, 0.01)

    val userAndMovieId = RatingUtil.getUserAndMovieId(sc)

    recommendDependOnUser(trainer, userAndMovieId)
    recommendDependOnMovie(trainer)

    session.close()

    }

}
