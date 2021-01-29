package com.esni.offlinerecommend.dao

import com.esni.recommendcommon.dao.RedisDao
import com.esni.recommendcommon.util.EnvironmentUtil
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

class MovieMatrixCacheDao extends RedisDao{

  private val jedis: Jedis = EnvironmentUtil.getLocalJedis

  def cacheMovieSimMatrix(movieMatrix: RDD[(Int, List[(Int, Double)])]): Unit = {

    val field = movieMatrix.collect()(0)
    val key = field._1.toString

    for (movieTup <- field._2) {
      writeHashSet(jedis, key, movieTup._1.toString, movieTup._2.toString)
    }

  }

}
