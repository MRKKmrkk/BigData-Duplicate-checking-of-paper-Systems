package com.esni.dataproduce;

import com.esni.dataproduce.bean.Producer;

import java.io.IOException;

public class Bootstrap {

    public static void main(String[] args) throws IOException {

        Producer producer = new Producer("data-producer.properties");
        producer.produce(true);

    }

}
