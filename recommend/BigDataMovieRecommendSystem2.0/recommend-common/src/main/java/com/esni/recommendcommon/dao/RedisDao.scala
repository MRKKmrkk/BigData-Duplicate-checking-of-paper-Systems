package com.esni.recommendcommon.dao

import java.util

import redis.clients.jedis.Jedis

trait RedisDao {

  def readList(jedis: Jedis, key: String, start: Long, end: Long): util.List[String] = {

    jedis.lrange(key, start, end)

  }

  def writeIntoList(jedis: Jedis, key: String, value: String): Unit = {

    jedis.lpush(key, value)

  }

}
