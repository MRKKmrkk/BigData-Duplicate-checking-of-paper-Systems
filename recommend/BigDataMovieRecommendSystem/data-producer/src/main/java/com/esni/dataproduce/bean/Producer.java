package com.esni.dataproduce.bean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class Producer {

    private Logger logger;
    private Properties properties;
    private HashMap<Integer, HashSet<Integer>> likeMap;
    private int movieStartIndex;
    private int movieEndIndex;
    private int userStarIndex;
    private int userEndIndex;

    public Producer(String resourceName) throws IOException {

        logger = LoggerFactory.getLogger("data-producer");
        properties = new Properties();

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        properties.load(loader.getResourceAsStream(resourceName));

        movieStartIndex = Integer.parseInt(properties.getProperty("movieid.min.index"));
        movieEndIndex = Integer.parseInt(properties.getProperty("movieid.max.index"));
        userStarIndex = Integer.parseInt(properties.getProperty("userid.min.index"));
        userEndIndex = Integer.parseInt(properties.getProperty("userid.max.index"));

        likeMap = new HashMap<Integer, HashSet<Integer>>();
        for (int i = userStarIndex; i <= userEndIndex; i++) {
            likeMap.put(i, new HashSet<Integer>());
        }



    }

    private int getRandomNumber(int min, int max) {

        return min + (int) (Math.random() * ((max - min) + 1));

    }

    /**
     * 将用户行为存储进日志
     */
    private void logUserBehavior(int userId, int movieId, String behavior) {

        logger.info("behavior:" + userId + "\t" + movieId + "\t" + behavior + "\t" + System.currentTimeMillis());

    }

    /**
     * 将用户评分写入日志
     */
    private void logScore(int userId, int movieId) {

        logger.info("rating:" + userId + "\t" + movieId + "\t" + getRandomNumber(0, 5) + "\t" + System.currentTimeMillis());

    }

    /**
     * 随机获取电影id
     */
    private int getRandomMovieId() {

        return getRandomNumber(movieStartIndex, movieEndIndex);

    }

    /**
     * 随机获取用户id
     */
    private int getRandomUserId() {

        return getRandomNumber(userStarIndex, userEndIndex);

    }

    /**
     * 获取随机行为
     */
    private void randomLogBehavior() {

        String behavior = null;
        int userID = getRandomUserId();
        int movieId = getRandomMovieId();

        while (behavior == null) {
            int key = getRandomNumber(0, 3);

            if (key == 0) {
                behavior = "click";
            }
            if (key == 1) {
                behavior = "search";
            }
            if (key == 2) {
                //如果电影已经被收藏，则不难再次收藏
                if (likeMap.get(userID).contains(movieId)) {
                    continue;
                }

                likeMap.get(userID).add(movieId);
                behavior = "like";
            }
            else{
                //如果电影未被收藏则不能取消收藏
                if (!likeMap.get(userID).contains(movieId)){
                    continue;
                }

                likeMap.get(userID).remove(movieId);
                behavior = "unlike";
            }
        }

        logUserBehavior(userID, movieId, behavior);

    }

    /**
     * 初始化评分数据，会遍历每一位用户并为他们随机对电影进行评分
     * 70%的用户会评分0-10部电影
     * 20%的用户会评分11-20部电影
     * 10%的用户会评分21-50部电影
     */
    private void initScore() {

        for (int userId = userStarIndex; userId <= userEndIndex; userId++) {

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
     */
    public void produce(boolean isInitScoreData) {

        //初始化评分数据
        if (isInitScoreData) {
            initScore();
        }

        for (int i = 0; i < 20000; i++) {
            //随机产生行为数据
            randomLogBehavior();
        }

    }

}
