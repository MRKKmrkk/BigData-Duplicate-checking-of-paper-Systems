package com.esni.recommendcommon.util;

import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.Properties;

public class RedisUtil {

    public static Jedis getJedis() throws IOException {

        Properties properties = PropertiesUtil.getProperties("redis.properties");
        return new Jedis(properties.getProperty("redis.host"), Integer.parseInt(properties.getProperty("redis.port")), 100000);

    }

    public static Jedis getJedis(Properties properties){

        return new Jedis(properties.getProperty("redis.host"), Integer.parseInt(properties.getProperty("redis.port")), 100000);

    }

}
