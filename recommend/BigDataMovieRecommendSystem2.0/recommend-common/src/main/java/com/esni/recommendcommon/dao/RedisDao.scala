package com.esni.recommendcommon.dao

import java.util

import redis.clients.jedis.Jedis

trait RedisDao {

  def isExist(jedis: Jedis, key: String, field: String): Boolean = {

    jedis.hexists(key, field)

  }

  def readValueFromHashSet(jedis: Jedis, key: String, field: String): String = {

    jedis.hget(key, field)

  }

  def writeHashSet(jedis: Jedis, key: String, field: String, value: String): Unit = {

    jedis.hset(key, field, value)

  }

  def readList(jedis: Jedis, key: String, start: Long, end: Long): util.List[String] = {

    jedis.lrange(key, start, end)

  }

  def writeIntoList(jedis: Jedis, key: String, value: String): Unit = {

    jedis.lpush(key, value)

  }

}
