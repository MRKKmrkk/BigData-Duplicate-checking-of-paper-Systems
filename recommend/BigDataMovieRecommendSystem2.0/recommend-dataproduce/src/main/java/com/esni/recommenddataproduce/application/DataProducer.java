package com.esni.recommenddataproduce.application;

import com.esni.recommenddataproduce.controller.DataProduceController;

import java.io.IOException;

public class DataProducer {

    public static void main(String[] args) throws IOException {

        DataProduceController controller = new DataProduceController();
        controller.dispatch();

    }

}
