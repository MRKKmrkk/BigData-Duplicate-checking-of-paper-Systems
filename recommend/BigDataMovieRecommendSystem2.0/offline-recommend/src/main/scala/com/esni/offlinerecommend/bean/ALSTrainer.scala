package com.esni.offlinerecommend.bean

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

/**
  * 此类属于SparkML库的二次封装
  * 便于调优和推荐算法的使用
  */
class ALSTrainer(ratings: RDD[Rating], iterations: Int=5) {

  private var model: MatrixFactorizationModel = null

  /**
    * 通过传入的ratings训练并获取模型
    * rank lambda这2个参数默认为配置文件中设置的值
    * iterations默认为5
    */
  def trainModel(rank: Int, lambda: Double): Unit = {

    model = ALS.train(ratings, rank, iterations, lambda)

  }

  /**
    * 通过模型获取预测值
    * 模型默认使用offline-recommend.properties中的参数,也可以自己设置参数
    */
  def predict(usersProducts: RDD[(Int, Int)],rank: Int, lambda: Double): RDD[Rating] = {

    trainModel(rank, lambda)
    model.predict(usersProducts)

  }

  def getModel: MatrixFactorizationModel = {

    model

  }

}
