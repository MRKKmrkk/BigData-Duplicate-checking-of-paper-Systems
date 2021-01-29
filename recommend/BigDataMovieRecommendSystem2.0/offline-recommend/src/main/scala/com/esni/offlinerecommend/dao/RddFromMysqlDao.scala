package com.esni.offlinerecommend.dao

import com.esni.offlinerecommend.bean.OfflineUserRecommend
import com.esni.recommendcommon.dao.DataFrameDao
import com.esni.recommendcommon.util.{EnvironmentUtil, PropertiesUtil}
import org.apache.spark.rdd.RDD

class RddFromMysqlDao extends DataFrameDao{

  private val session = EnvironmentUtil.getSparkSession
  private val properties = PropertiesUtil.getProperties("database.properties")

  def getUserAndMovieIdWithRdd(): RDD[(Int, Int)] = {

    val userId = readDataFrame(session, "user_info", properties)
      .rdd
      .map(x => x.getInt(1))

    val movieId = readDataFrame(session, "movie_info", properties)
      .rdd
      .map(x => x.getInt(1))

    userId.cartesian(movieId)

  }

  def saveRecommenderRddInMysql(rdd: RDD[(Int, List[(Int, Double)])]): Unit = {

    import session.implicits._
    val df = rdd
      .map{case (k, l) => OfflineUserRecommend(k, l(0)._1, l(1)._1, l(2)._1, l(3)._1, l(4)._1, l(5)._1, l(6)._1, l(7)._1, l(8)._1, l(9)._1)}
      .toDF()

    writeDataFrames(df, "offline_user_recommend", properties)

  }

}
