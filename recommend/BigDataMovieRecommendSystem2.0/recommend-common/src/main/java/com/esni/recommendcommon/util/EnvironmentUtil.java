package com.esni.recommendcommon.util;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

public class EnvironmentUtil {

    private static ThreadLocal<SparkContext> localSparkContext;
    private static ThreadLocal<SparkSession> localSparkSession;
    private static ThreadLocal<String[]> localArguments;

    static {

        localSparkContext = new ThreadLocal<SparkContext>();
        localSparkSession = new ThreadLocal<SparkSession>();
        localArguments = new ThreadLocal<String[]>();

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

}
