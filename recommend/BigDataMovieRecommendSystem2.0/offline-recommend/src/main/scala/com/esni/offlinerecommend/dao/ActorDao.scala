package com.esni.offlinerecommend.dao

import com.esni.recommendcommon.dao.RddDao
import com.esni.recommendcommon.util.EnvironmentUtil
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

class ActorDao extends RddDao{

  private val sc: SparkContext = EnvironmentUtil.getSparkContext
  private val args: Array[String] = EnvironmentUtil.getArguments

  def getActorRdd(): RDD[Rating] = {

    val scoreRdd = readScoreRdd(sc, args(0))
      .map(x => ( (x._1, x._2), (x._3.toDouble, x._4) ))
      .groupByKey()
      .map{x =>
        val maxTup = x._2.toList.sortWith(_._2 > _._2).head
        (x._1._1, x._1._2, maxTup._1, maxTup._2)
      }

    val behaviorRdd = readBehaviorRdd(sc, args(1))
      .map{x =>
        var score = 0.0
        if ("click".equals(x._3)) {
          score = 1.0
        }
        if ("search".equals(x._3)) {
          score = 2.0
        }
        if ("like".equals(x._3)) {
          score = 2.0
        }
        if ("unlike".equals(x._3)) {
          score = -2.0
        }
        ((x._1, x._2, score), x._4)
      }
      .groupByKey()
      .map(x => (x._1._1, x._1._2, x._1._3, x._2.toList.sortWith(_ > _).head))

    // 计算偏好值，封装为rating
    scoreRdd.union(behaviorRdd)
      .map(x => ((x._1, x._2), x._3))
      .reduceByKey(_+_)
      .map{case (a,b) =>
        Rating(a._1, a._2, b)
      }

  }

}
