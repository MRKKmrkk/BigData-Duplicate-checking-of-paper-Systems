package com.esni.offlinerecommend.als

import java.io.InputStream
import java.util.Properties

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

/**
  * 此类属于SparkML库的二次封装
  * 便于于调优和推荐算法的使用
  */
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

  /**
    * 通过模型获取预测值
    * 模型默认使用offline-recommend.properties中的参数,也可以自己设置参数
    */
  def predict(usersProducts: RDD[(Int, Int)],rank: Int=properties.getProperty("model.arg.rank").toInt,
              lambda: Double=properties.getProperty("model.arg.lambda").toDouble): RDD[Rating] = {
    val model = trainModel(rank, lambda)
    model.predict(usersProducts)

  }

}
