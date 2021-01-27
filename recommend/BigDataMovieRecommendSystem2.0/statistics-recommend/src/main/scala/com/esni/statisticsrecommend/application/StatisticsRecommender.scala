package com.esni.statisticsrecommend.application

import com.esni.recommendcommon.common.RecommenderApplication
import com.esni.statisticsrecommend.controller.StatisticsRecommendController

object StatisticsRecommender extends RecommenderApplication{

  def main(args: Array[String]): Unit = {

    val arg = Array("D:\\Projects\\dpystem\\recommend\\testData\\ur.log")

    start("Statistics Recommend", arg, createSession = true){

      val controller = new StatisticsRecommendController
      controller.dispatch()

    }

  }

}
