package com.esni.recommenddataproduce.dao;

import com.esni.recommendcommon.common.RecommenderDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogDao implements RecommenderDao {

    private Logger logger;

    public LogDao() {

        logger = LoggerFactory.getLogger("data-produce");

    }

    /**
     * 将用户行为存储进日志
     */
    public void logUserBehavior(int userId, int movieId, String behavior) {

        logger.info("behavior:" + userId + "\t" + movieId + "\t" + behavior + "\t" + System.currentTimeMillis());

    }

    /**
     * 将用户评分写入日志
     */
    public void logScore(int userId, int movieId, int score) {

        logger.info("score:" + userId + "\t" + movieId + "\t" + score + "\t" + System.currentTimeMillis());

    }

}
