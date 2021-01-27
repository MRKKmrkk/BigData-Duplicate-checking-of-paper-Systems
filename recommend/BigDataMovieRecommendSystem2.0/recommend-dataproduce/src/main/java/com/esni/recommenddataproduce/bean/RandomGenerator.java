package com.esni.recommenddataproduce.bean;

public class RandomGenerator {

    private int minUserIndex;
    private int maxUserIndex;
    private int minMovieIndex;
    private int maxMovieIndex;

    public RandomGenerator(int minUserIndex, int maxUserIndex, int minMovieIndex, int maxMovieIndex) {

        this.minUserIndex = minUserIndex;
        this.maxUserIndex = maxUserIndex;
        this.minMovieIndex = minMovieIndex;
        this.maxMovieIndex = maxMovieIndex;

    }

    public int getRandomNumber(int min, int max) {

        return min + (int) (Math.random() * ((max - min) + 1));

    }

    /**
     * 随机获取电影id
     */
    public int getRandomMovieId() {

        return getRandomNumber(minMovieIndex, maxMovieIndex);

    }

    /**
     * 随机获取用户id
     */
    public int getRandomUserId() {

        return getRandomNumber(minUserIndex, maxUserIndex);

    }


}
