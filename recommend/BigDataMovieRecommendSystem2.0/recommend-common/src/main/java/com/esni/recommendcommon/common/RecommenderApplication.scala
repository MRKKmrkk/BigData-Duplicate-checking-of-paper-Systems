package com.esni.recommendcommon.common

import com.esni.recommendcommon.util.EnvironmentUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

trait RecommenderApplication {

  def start(appName: String, args: Array[String], createSession: Boolean=false)(operation: => Unit): Unit = {

    EnvironmentUtil.setArguments(args)
    val conf: SparkConf = new SparkConf().setAppName(appName).setMaster("local[*]")
    var sc: SparkContext = null
    var session: SparkSession = null

    if (createSession){
      session = SparkSession.builder().config(conf).getOrCreate()
      sc = session.sparkContext
      EnvironmentUtil.setSparkContext(sc)
      EnvironmentUtil.setSparkSession(session)
    }
    else{
      sc = new SparkContext(conf)
      EnvironmentUtil.setSparkContext(sc)
    }

    try {
      operation
    }
    catch {
      case ex => println(ex.getMessage)
    }

    if (session != null){
      session.close()
    }
    sc.stop()

  }


}
