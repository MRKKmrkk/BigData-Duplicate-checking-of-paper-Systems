package com.esni.offlinerecommend.als

import breeze.numerics.sqrt
import com.esni.offlinerecommend.utils.RatingUtil
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 参数调优工具
  * 会输出所有所选参数中rmse最小的
  */

object ArgumentsTuning {

  val conf: SparkConf = new SparkConf().setAppName("Arguments Tuning").setMaster("local[*]")
  val sc = new SparkContext(conf)

  def getMinRmse(ratings: RDD[Rating]): (Int, Double, Double) = {

    val trainer = new ALSTrainer(ratings)
    val realRatings = getRealRatings(ratings)
    val userAndMovieId = getUserAndMovieId(ratings)

    val rmses = for (rank <- Array(60, 70, 80, 90); lambda <- Array(1, 0.1, 0.01)) yield {
      val predictRatings = trainer.predict(userAndMovieId, rank, lambda)
          .map(x => ((x.user, x.product), x.rating))

      val rmse: Double = sqrt(
        realRatings.join(predictRatings).map {
          case ((uid, mid), (real, pre)) =>
            val err = real - pre
            err * err
        }.mean()
      )
      (rank, lambda, rmse)
    }

    rmses.minBy(_._3)

  }

  def getUserAndMovieId(ratings: RDD[Rating]): RDD[(Int, Int)] = {

    ratings
      .map(x => (x.user, x.product))

  }

  def getRealRatings(ratings: RDD[Rating]): RDD[((Int, Int), Double)] = {

    ratings
      .map(x => ((x.user, x.product), x.rating))

  }

  def adjustASLParams(scoresLogFilePath: String, behaviorLogFilePath: String): Unit = {

    val ratings = RatingUtil.getRatings(scoresLogFilePath, behaviorLogFilePath, sc)
    println(getMinRmse(ratings))

  }

  def main(args: Array[String]): Unit = {

    adjustASLParams("D:\\Projects\\dpystem\\recommend\\testData\\ur.log",
      "D:\\Projects\\dpystem\\recommend\\testData\\ub.log")

  }

}
