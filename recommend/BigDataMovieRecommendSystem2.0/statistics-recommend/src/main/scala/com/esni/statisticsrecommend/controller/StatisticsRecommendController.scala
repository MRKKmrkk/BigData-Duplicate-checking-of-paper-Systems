package com.esni.statisticsrecommend.controller

import com.esni.recommendcommon.common.RecommenderController
import com.esni.statisticsrecommend.service.StatisticsRecommendService

class StatisticsRecommendController extends RecommenderController{

  val service = new StatisticsRecommendService

  override def dispatch(): Unit = {

    service.execute()

  }

}
