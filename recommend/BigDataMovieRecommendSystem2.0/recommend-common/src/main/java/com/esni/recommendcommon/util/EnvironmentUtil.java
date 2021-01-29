package com.esni.recommendcommon.util;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import redis.clients.jedis.Jedis;

public class EnvironmentUtil {

    private static ThreadLocal<SparkContext> localSparkContext;
    private static ThreadLocal<SparkSession> localSparkSession;
    private static ThreadLocal<String[]> localArguments;
    private static ThreadLocal<Jedis> localJedis;

    static {

        localSparkContext = new ThreadLocal<SparkContext>();
        localSparkSession = new ThreadLocal<SparkSession>();
        localArguments = new ThreadLocal<String[]>();
        localJedis = new ThreadLocal<Jedis>();

    }

    public static void setSparkContext(SparkContext sparkContext) {

        localSparkContext.set(sparkContext);

    }

    public static SparkContext getSparkContext() {

        return localSparkContext.get();

    }

    public static void removeSparkContext(){

        localSparkContext.remove();

    }

    public static void setSparkSession(SparkSession session) {

        localSparkSession.set(session);

    }

    public static SparkSession getSparkSession() {

        return localSparkSession.get();

    }

    public static void removeSparkSession(){

        localSparkSession.remove();

    }

    public static void setArguments(String[] args) {

        localArguments.set(args);

    }

    public static String[] getArguments() {

        return localArguments.get();

    }

    public static void removeArguments() {

        localArguments.remove();

    }

    public static void setLocalJedis(Jedis jedis) {

        localJedis.set(jedis);

    }

    public static Jedis getLocalJedis() {

        return localJedis.get();

    }

    public static void removeLocalJedis() {

        localJedis.remove();

    }

}
