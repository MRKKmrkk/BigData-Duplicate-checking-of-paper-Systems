package com.esni.recommenddataproduce.service;

import com.esni.recommendcommon.common.RecommenderService;
import com.esni.recommendcommon.util.PropertiesUtil;
import com.esni.recommenddataproduce.bean.RandomGenerator;
import com.esni.recommenddataproduce.dao.LogDao;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

public class DataProduceService implements RecommenderService {

    private LogDao dao;
    private boolean isInitScoreData;
    private boolean isTestMode;
    private int testNumber;
    private RandomGenerator random;
    private int userStartIndex;
    private int userEndIndex;
    private int movieStartIndex;
    private int movieEndIndex;
    private HashMap<Integer, HashSet<Integer>> likeMap;

    public DataProduceService() throws IOException {

        dao = new LogDao();

        Properties properties = PropertiesUtil.getProperties("dataproduce.properties");
        isInitScoreData = Integer.parseInt(properties.getProperty("producer.initScoreData")) == 1;
        isTestMode = Integer.parseInt(properties.getProperty("producer.testMode")) == 1;
        testNumber = Integer.parseInt(properties.getProperty("producer.testNumber"));
        userStartIndex = Integer.parseInt(properties.getProperty("producer.user.startIndex"));
        userEndIndex = Integer.parseInt(properties.getProperty("producer.user.endIndex"));
        movieStartIndex = Integer.parseInt(properties.getProperty("producer.movie.startIndex"));
        movieEndIndex = Integer.parseInt(properties.getProperty("producer.movie.endIndex"));

        random = new RandomGenerator(userStartIndex, userEndIndex, movieStartIndex, movieEndIndex);

        likeMap = new HashMap<Integer, HashSet<Integer>>();
        for (int i = userStartIndex; i <= userEndIndex; i++) {
            likeMap.put(i, new HashSet<Integer>());
        }

    }

    private void initScore() {

        for (int userId = userStartIndex; userId <= userEndIndex; userId++) {

            int userLevel = random.getRandomNumber(1, 10);
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

            for (int i = 0; i < random.getRandomNumber(s, e); i++) {
                dao.logScore(userId, random.getRandomMovieId(), random.getRandomNumber(0, 5));
            }

        }
    }

    private void randomLogBehavior() {

        String behavior = null;
        int userID = random.getRandomUserId();
        int movieId = random.getRandomMovieId();

        while (behavior == null) {
            int key = random.getRandomNumber(0, 3);

            if (key == 0) {
                behavior = "click";
            }
            if (key == 1) {
                behavior = "search";
            }
            if (key == 2) {
                //如果电影已经被收藏，则不再次收藏
                if (likeMap.get(userID).contains(movieId)) {
                    continue;
                }

                likeMap.get(userID).add(movieId);
                behavior = "like";
            }
            if (key == 3){
                //如果电影未被收藏则不能取消收藏
                if (!likeMap.get(userID).contains(movieId)){
                    continue;
                }

                likeMap.get(userID).remove(movieId);
                behavior = "unlike";
            }
            if (key == 4){
                dao.logScore(userID, random.getRandomMovieId(), random.getRandomNumber(0, 5));
            }
        }

        dao.logUserBehavior(userID, movieId, behavior);

    }

    /**
     * 模拟生产用户日志
     */
    public void execute() {

        //初始化评分数据
        if (isInitScoreData) {
            initScore();
        }

        // 随机生成用户行为
        int count = 0;
        while (true) {

            if (isTestMode && count >= testNumber) {
                break;
            }

            randomLogBehavior();

            count++;

        }

    }

}


