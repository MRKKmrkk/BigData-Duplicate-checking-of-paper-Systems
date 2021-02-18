package com.esni.offlinerecommend.service

import breeze.numerics.sqrt
import com.esni.offlinerecommend.bean.ALSTrainer
import com.esni.offlinerecommend.dao.{ActorDao, RddFromMysqlDao}
import com.esni.recommendcommon.common.RecommenderService
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD


class ArgumentsTuningService(rddFromMysqlDao: RddFromMysqlDao) extends RecommenderService{

  private val actorDao: ActorDao = new ActorDao
  private var model: MatrixFactorizationModel = _
  private val ratings: RDD[Rating] = actorDao.getActorRdd()

  val userAndMovieId: RDD[(Int, Int)] = rddFromMysqlDao.getUserAndMovieIdWithRdd()


  def getRealRatings(ratings: RDD[Rating]): RDD[((Int, Int), Double)] = {

    ratings
      .map(x => ((x.user, x.product), x.rating))

  }

  def adjustASLParams(): Unit = {

    val trainer = new ALSTrainer(ratings)
    val realRatings = getRealRatings(ratings)


//    val rmses = for (rank <- Array(60, 70, 80, 90); lambda <- Array(1, 0.1, 0.01)) yield {
    val rmses = for (rank <- Array(60); lambda <- Array(1)) yield {
      val predictRatings = trainer.predict(userAndMovieId, rank, lambda)
        .map(x => ((x.user, x.product), x.rating))

      val rmse: Double = sqrt(
        realRatings.join(predictRatings).map {
          case ((uid, mid), (real, pre)) =>
            val err = real - pre
            err * err
        }.mean()
      )
      (rmse, trainer.getModel)
    }

    model = rmses.minBy(_._1)._2

  }

  def getOptimalModel(): MatrixFactorizationModel = {

    model

  }

  override def execute(): Unit = {

    adjustASLParams()

  }

}
