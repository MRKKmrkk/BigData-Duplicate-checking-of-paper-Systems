package com.esni.offlinerecommend.service

import com.esni.offlinerecommend.bean.ALSTrainer
import com.esni.offlinerecommend.dao.{MovieMatrixCacheDao, RddFromMysqlDao}
import com.esni.recommendcommon.common.RecommenderService
import com.esni.recommendcommon.util.PropertiesUtil
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix

class OfflineRecommderService(model: MatrixFactorizationModel, userAndMovieId: RDD[(Int, Int)], rddFromMysqlDao: RddFromMysqlDao) extends RecommenderService{

  private val cacheDao: MovieMatrixCacheDao = new MovieMatrixCacheDao

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
  def recommendDependOnUser(): Unit = {

    val result = model.predict(userAndMovieId)
      .map(x => (x.user, (x.product, x.rating)))
      .groupByKey()
      .map(x => (x._1, x._2.toList.sortWith(_._2 > _._2).take(10)))

    rddFromMysqlDao.saveRecommenderRddInMysql(result)

  }

  /**
    * 基于电影的推荐
    * 将计算电影相似度矩阵，并将矩阵缓存进redis中
    */
  def recommendDependOnMovie(trainer: ALSTrainer): Unit = {

    val moviesFeature = model
      .productFeatures
      .map{
        case (movieId, features) =>
          (movieId, new DoubleMatrix(features))
      }

    val carMovieFeature = moviesFeature
      .cartesian(moviesFeature)
      .filter{case (mf1, mf2) => mf1._1 != mf2._1}

    val result = carMovieFeature
      .map{case (mf1, mf2) => (mf1._1, (mf2._1, consineSim(mf1._2, mf2._2)))}
      .filter(_._2._2 > PropertiesUtil.getProperties("offline-recommend.properties").getProperty("sim.filter.threshold").toDouble)
      .groupByKey()
      .map(item => (item._1, item._2.toList.sortWith(_._2 > _._2)))
    
    cacheDao.cacheMovieSimMatrix(result)

  }

  override def execute(): Unit = {

    recommendDependOnUser()


  }

}
