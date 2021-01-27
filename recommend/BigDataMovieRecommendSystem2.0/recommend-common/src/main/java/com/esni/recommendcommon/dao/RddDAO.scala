package com.esni.recommendcommon.dao

import com.esni.recommendcommon.common.RecommenderDao
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

trait RddDAO extends RecommenderDao {

  def readScoreRdd(sc: SparkContext, path: String): RDD[(Int, Int, Int, Long)] = {

    sc
      .textFile(path)
      .map(_.split("\t"))
      .map(l => (l(0).toInt, l(1).toInt, l(2).toInt, l(3).toLong))

  }

  def readBehaviorRdd(sc: SparkContext, path: String): RDD[(Int, Int, String, Long)] = {

    sc
      .textFile(path)
      .map(_.split("\t"))
      .map(l => (l(0).toInt, l(1).toInt, l(2), l(3).toLong))

  }

}