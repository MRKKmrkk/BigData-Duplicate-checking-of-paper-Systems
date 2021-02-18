package com.esni.offlinerecommend.application

import com.esni.offlinerecommend.controller.OfflineRecommderController
import com.esni.recommendcommon.common.RecommenderApplication

object OfflineRecommderApplication extends RecommenderApplication{

  def main(args: Array[String]): Unit = {

    start("Offline Recommender", Array("D:\\Projects\\dpystem\\recommend\\testData\\ur.log", "D:\\Projects\\dpystem\\recommend\\testData\\ub.log"), createSession = true){

      val controller = new OfflineRecommderController()
      controller.dispatch()

    }

  }

}
