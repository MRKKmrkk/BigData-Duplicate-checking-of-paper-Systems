package com.esni;

import com.esni.dataproducer.Producer;

import java.io.IOException;

public class Bootstrap {

    public static void main(String[] args) throws IOException {

        Producer producer = new Producer("data-producer.properties");
        producer.produce(true);

    }

}
