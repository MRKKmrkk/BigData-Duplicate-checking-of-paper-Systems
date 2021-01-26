package com.esni.dataproduce;

import redis.clients.jedis.Jedis;

import java.io.*;

public class RedisDemo {

    public static void main(String[] args) throws IOException {

        Jedis jedis = new Jedis("hadoop03", 6379);

        BufferedReader reader = new BufferedReader(new FileReader(new File("D:\\Projects\\dpystem\\recommend\\testData\\ur.log")));
        String line = null;
        while ((line = reader.readLine()) != null) {
            String[] fields = line.split("\t");
            jedis.lpush("user_id:" + fields[0], fields[1] + ":" + fields[2]);
        }

        jedis.close();

    }

}
