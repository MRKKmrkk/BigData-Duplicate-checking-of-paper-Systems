package com.esni.recommenddataproduce.controller;

import com.esni.recommendcommon.common.RecommenderController;
import com.esni.recommenddataproduce.service.DataProduceService;

import java.io.IOException;

public class DataProduceController implements RecommenderController {

    private DataProduceService service;

    public DataProduceController() throws IOException {

        service = new DataProduceService();

    }

    public void dispatch() {

        service.execute();

    }

}


