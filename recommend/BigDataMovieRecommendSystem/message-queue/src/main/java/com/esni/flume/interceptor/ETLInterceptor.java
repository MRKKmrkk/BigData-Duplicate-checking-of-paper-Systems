package com.esni.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * Flume拦截器
 * 从日志文件中筛选出用户日志,加以清洗
 * 并按照用户行为或投票行为为事件增加头部，用于Channel选择器进行区分
 * 将评分行为存入Redis
 */
public class ETLInterceptor implements Interceptor {
    private HashMap<String, String> behaviorActor;
    private HashMap<String, String> ratingActor;
    private ArrayList<Event> eventList;
    private Jedis jedis;

    public void initialize() {

        eventList = new ArrayList<Event>();

        behaviorActor = new HashMap<String, String>();
        behaviorActor.put("actor", "behavior");

        ratingActor = new HashMap<String, String>();
        ratingActor.put("actor", "rating");

        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("redis.properties");
        Properties properties = new Properties();
        try {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        jedis = new Jedis(properties.getProperty("redis.host"), Integer.parseInt(properties.getProperty("redis.port")));

    }

    /*
     * 变量名命名不规范
     * 需要将rating修改为score
     */
    public Event intercept(Event event) {

        String line = new String(event.getBody());
        if (line.contains("behavior:")) {
            event.setBody(line.split("behavior:")[1].getBytes());
            event.setHeaders(behaviorActor);
            return event;
        }
        if (line.contains("rating:")) {
            String score = line.split("rating:")[1];
            String[] fields = score.split("\t");

            jedis.lpush("user_id:" + fields[0], fields[1] + ":" + fields[2]);

            event.setBody(score.getBytes());
            event.setHeaders(ratingActor);
        }

        return event;

    }

    public List<Event> intercept(List<Event> list) {
        eventList.clear();

        for (Event event : list) {
            eventList.add(intercept(event));
        }

        return eventList;

    }

    /**
     * 关闭Redis连接
     */
    public void close() {

        jedis.close();

    }

    public static class Builder implements Interceptor.Builder {

        public Interceptor build() {

            return new ETLInterceptor();

        }

        public void configure(Context context) {



        }

    }

}
