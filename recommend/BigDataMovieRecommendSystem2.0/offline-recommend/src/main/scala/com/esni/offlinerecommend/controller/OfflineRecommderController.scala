package com.esni.offlinerecommend.controller

import com.esni.offlinerecommend.dao.RddFromMysqlDao
import com.esni.offlinerecommend.service.{ArgumentsTuningService, OfflineRecommderService}
import com.esni.recommendcommon.common.RecommenderController

class OfflineRecommderController extends RecommenderController{

  override def dispatch(): Unit = {

    val rddFromMysqlDao: RddFromMysqlDao = new RddFromMysqlDao

    val argsTuning: ArgumentsTuningService = new ArgumentsTuningService(rddFromMysqlDao)
    argsTuning.execute()

    val offlineRecommend: OfflineRecommderService = new OfflineRecommderService(argsTuning.getOptimalModel(), argsTuning.userAndMovieId, rddFromMysqlDao)
    offlineRecommend.execute()

  }



}
