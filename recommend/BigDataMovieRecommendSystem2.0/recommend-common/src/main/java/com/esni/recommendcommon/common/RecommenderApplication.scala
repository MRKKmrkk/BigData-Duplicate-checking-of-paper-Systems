package com.esni.recommendcommon.common

import com.esni.recommendcommon.util.{EnvironmentUtil, PropertiesUtil, RedisUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

trait RecommenderApplication {

  def start(appName: String, args: Array[String], createSession: Boolean=false, createJedis: Boolean=false)(operation: => Unit): Unit = {

    EnvironmentUtil.setArguments(args)
    val conf: SparkConf = new SparkConf().setAppName(appName).setMaster("local[*]")
    var sc: SparkContext = null
    var session: SparkSession = null
    var jedis: Jedis = null

    if (createJedis){
      jedis = RedisUtil.getJedis(PropertiesUtil.getProperties("redis.properties"))
      EnvironmentUtil.setLocalJedis(jedis)
    }
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
    if (jedis != null){
      jedis.close()
    }
    sc.stop()

  }


}
