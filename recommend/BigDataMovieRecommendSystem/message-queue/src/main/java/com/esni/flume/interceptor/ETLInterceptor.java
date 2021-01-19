package com.esni.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Flume拦截器
 * 从日志文件中筛选出用户日志,加以清洗
 * 并按照用户行为或投票行为为事件增加头部，用于Channel选择器进行区分
 */
public class ETLInterceptor implements Interceptor {
    private HashMap<String, String> behaviorActor;
    private HashMap<String, String> ratingActor;
    private ArrayList<Event> eventList;

    public void initialize() {

        eventList = new ArrayList<Event>();

        behaviorActor = new HashMap<String, String>();
        behaviorActor.put("actor", "behavior");

        ratingActor = new HashMap<String, String>();
        ratingActor.put("actor", "rating");

    }

    public Event intercept(Event event) {

        String line = new String(event.getBody());
        if (line.contains("behavior:")) {
            event.setBody(line.split("behavior:")[1].getBytes());
            event.setHeaders(behaviorActor);
            return event;
        }
        if (line.contains("rating:")) {
            event.setBody(line.split("rating:")[1].getBytes());
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

    public void close() {



    }

    public static class Builder implements Interceptor.Builder {

        public Interceptor build() {

            return new ETLInterceptor();

        }

        public void configure(Context context) {



        }

    }

}
