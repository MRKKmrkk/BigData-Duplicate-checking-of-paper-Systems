package com.esni.dataproduce.bean;

import com.esni.dataproduce.util.RedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

public class NewProduce {

    private Logger logger;
    private Properties properties;
    private HashMap<Integer, HashSet<Integer>> likeMap;
    private int movieStartIndex;
    private int movieEndIndex;
    private int userStarIndex;
    private int userEndIndex;

    public NewProduce(String resourceName) throws IOException {

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
     * 并缓存到redis
     */
    private void logScore(int userId, int movieId) {

        int score = getRandomNumber(0, 5);
        logger.info("rating:" + userId + "\t" + movieId + "\t" + score + "\t" + System.currentTimeMillis());
        RedisUtil.cacheScore(userId, movieId, score);

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
    private void randomLogBehavior(int userID, int movieId) {

        String behavior = null;
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
            else {
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
     * 70%的用户会评分1-30部电影
     * 20%的用户会评分31-60部电影
     * 10%的用户会评分61-100部电影
     *
     * 初始化用户行为
     * 70%的用户会产生30-100次行为
     * 20%的用户会产生101-300次行为
     * 10%的用户会产生301-500次行为
     */
    public void initData() {

        for (int userId = userStarIndex; userId <= userEndIndex; userId++) {

            int userLevel = getRandomNumber(1, 10);
            int scoreStart = 61;
            int scoreEnd = 100;
            int behaviorStart = 301;
            int behaviorEnd = 500;

            if (userLevel <= 7) {
                scoreStart = 1;
                scoreEnd = 31;
                behaviorStart = 30;
                behaviorEnd = 100;
            }
            else if (userLevel <= 9) {
                scoreStart = 31;
                scoreEnd = 60;
                behaviorStart = 101;
                behaviorEnd = 300;
            }

            for (int i = 0; i < getRandomNumber(scoreStart, scoreEnd); i++) {
                logScore(userId, getRandomMovieId());
            }
            for (int i = 0; i < getRandomNumber(behaviorStart, behaviorEnd); i++) {
                randomLogBehavior(userId, getRandomMovieId());
            }

        }

    }

    /**
     * 模拟生产用户日志
     */
    public void produce(boolean isInitData) {

        //初始化评分数据
        if (isInitData) {
            initData();
        }

        // 随机产生用户行为和电影评分
        // 产生电影评分的概率为1/4
        while (true){
            int flag = getRandomNumber(1, 10);
            if (flag > 2) {
                logScore(getRandomUserId(), getRandomMovieId());
            }
            else{
                randomLogBehavior(getRandomUserId(), getRandomMovieId());
            }
        }

    }

}
