package com.esni.offlinerecommend

import com.esni.offlinerecommend.als.ALSTrainer
import com.esni.offlinerecommend.utils.RatingUtil
import org.apache.spark.{SparkConf, SparkContext}

object OfflineRecommender {

  val conf: SparkConf = new SparkConf().setAppName("offline recommender").setMaster("local[*]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    val ratings = RatingUtil.getRatings("D:\\Projects\\dpystem\\recommend\\testData\\ur.log",
      "D:\\Projects\\dpystem\\recommend\\testData\\ub.log", sc)

    val userAndMovieId = RatingUtil.getUserAndMovieId

    val trainer = new ALSTrainer(ratings)

    trainer.predict(userAndMovieId).foreach(println)

    }

}
