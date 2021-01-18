package com.esni.dataproducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

public class Producer {

    private Logger logger;
    private Properties properties;
    private Random random;

    public Producer(String resourceName) throws IOException {

        logger = LoggerFactory.getLogger("data-producer");
        properties = new Properties();

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        properties.load(loader.getResourceAsStream(resourceName));

        random = new Random();

    }

    private int getRandomNumber(int min, int max) {

        return random.nextInt(max - min + max) + min;

    }

    /**
     * 将用户行为存储进日志
     */
    private void logUserBehavior(int userId, String behavior) {

        logger.info(userId + "\t" + behavior + "\t" + System.currentTimeMillis());

    }

    /**
     * 将用户评分写入日志
     */
    private void logScore(int userId, int movieId) {

        logger.info(userId + "\t" + movieId + "\t" + getRandomNumber(0, 5) + "\t" + System.currentTimeMillis());

    }

    /**
     * 随机获取电影id
     */
    private int getRandomMovieId() {

        int startIndex = Integer.parseInt(properties.getProperty("movieid.min.index"));
        int endIndex = Integer.parseInt(properties.getProperty("movieid.max.index"));

        return getRandomNumber(startIndex, endIndex);

    }

    /**
     * 随机获取用户id
     */
    private int getRandomUserId() {

        int startIndex = Integer.parseInt(properties.getProperty("userid.min.index"));
        int endIndex = Integer.parseInt(properties.getProperty("userid.max.index"));

        return getRandomNumber(startIndex, endIndex);

    }

    /**
     * 获取随机行为
     */
//    private String getRandomBehavior() {
//
//        int key = getRandomNumber(0, 3);
//        switch (key){
//            case 0:
//                return "click";
//                break;
//            case 1:
//                return "search";
//                break;
//            case 2:
//                return "like";
//                break;
//            case 3:
//                return "unlike"
//
//        }
//
//    }

    /**
     * 初始化评分数据，会遍历每一位用户并为他们随机对电影进行评分
     * 70%的用户会评分0-10部电影
     * 20%的用户会评分11-20部电影
     * 10%的用户会评分21-50部电影
     */
    private void initScore() {

        int startIndex = Integer.parseInt(properties.getProperty("userid.min.index"));
        int endIndex = Integer.parseInt(properties.getProperty("userid.max.index"));

        for (int userId = startIndex; userId <= endIndex; userId++) {

            int userLevel = getRandomNumber(1, 10);
            int s = 21;
            int e = 50;

            if (userLevel <= 7) {
                s = 0;
                e = 10;
            }
            else if (userLevel <= 9) {
                s = 11;
                e = 20;
            }

            for (int i = 0; i < getRandomNumber(s, e); i++) {
                logScore(userId, getRandomMovieId());
            }

        }

    }

    /**
     * 模拟生产用户日志
     * 每次启动时默认对评分数据进行初始化
     */
    public void produce() {

        //初始化评分数据
        initScore();

        for (int i = 0; i < 50; i++) {
            //随机产生行为数据
            logUserBehavior(getRandomUserId(), );
        }

    }

}
