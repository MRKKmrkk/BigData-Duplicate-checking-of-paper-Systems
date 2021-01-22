package com.esni.offlinerecommend.als

import java.io.InputStream
import java.util.Properties

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

//object ALSUtil {
//
//  // 读取als-model.properties配置文件
//  private val in: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("offline-recommend.properties")
//  val properties = new Properties()
//  properties.load(in)
//
//  /**
//    * 通过传入的ratings训练并获取模型
//    * rank iterations lambda这三个参数默认为配置文件中设置的值
//    */
//  def tarinModel(ratings: RDD[Rating], rank: Int=properties.getProperty("mode.arg.rank").toInt,
//                 iterations: Int=properties.getProperty("mode.arg.iterations").toInt,
//                 lambda: Double=properties.getProperty("mode.arg.lambda").toDouble): MatrixFactorizationModel = {
//
//    ALS.train(ratings, rank, iterations, lambda)
//
//  }
//
//  def predict(model: MatrixFactorizationModel, usersProducts: RDD[(Int, Int)]): RDD[Rating] = {
//
//    model.predict(usersProducts)
//
//  }
//
//}

class ALSTrainer(ratings: RDD[Rating]) {

  // 读取als-model.properties配置文件
  private val in: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("offline-recommend.properties")
  val properties = new Properties()
  properties.load(in)

  /**
    * 通过传入的ratings训练并获取模型
    * rank lambda这三个参数默认为配置文件中设置的值
    * iterations默认为5
    */
  def trainModel(rank: Int, lambda: Double): MatrixFactorizationModel = {

    ALS.train(ratings, rank, properties.getProperty("model.arg.iterations").toInt, lambda)

  }

  def predict(usersProducts: RDD[(Int, Int)],rank: Int=properties.getProperty("model.arg.rank").toInt,
              lambda: Double=properties.getProperty("model.arg.lambda").toDouble): RDD[Rating] = {
    val model = trainModel(rank, lambda)
    model.predict(usersProducts)

  }

}
