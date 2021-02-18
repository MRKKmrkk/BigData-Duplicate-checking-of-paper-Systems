package com.esni.dataproduce.util;

import redis.clients.jedis.Jedis;

public class RedisUtil {

    private static Jedis jedis;

    static {

        jedis = new Jedis("hadoop03", 6379);

    }

    public static void cacheScore(int userId, int movieId, int score) {

        jedis.lpush("user_id:" + userId, movieId + ":" + score);

    }

    public static void close() {

        jedis.close();

    }

}
