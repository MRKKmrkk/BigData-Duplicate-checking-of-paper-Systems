package com.esni.realtimerecommend.util

import com.esni.realtimerecommend.RealTimeRecommenderDemo
import scala.collection.JavaConversions._

object Demo {

  def computeSim(movieId: Int, recentMovieId: Int): Double = {

    val map = MysqlUtil.getMovieMatrixAsMap(271)
    if (map.nonEmpty) {
      map.get(recentMovieId) match {
        case Some(sim) => sim
        case None => 0.0
      }
    }
    else{
      0.0
    }

  }

  def main(args: Array[String]): Unit = {

    val simMovies: Array[Int] = RealTimeRecommenderDemo.getTopSimMovie(25, 10)
      .map(x => x._1)
    val recentScoreMovies: Array[(Int, Double)] = RealTimeRecommenderDemo.getUserRecentScore(735, 10)

    // 存放电影与优先级
    val prioritys = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    //存放增强因子
    val in = scala.collection.mutable.HashMap[Int, Int]()
    // 存放减弱因子
    val de = scala.collection.mutable.HashMap[Int, Int]()

    for (top <- simMovies; recentScore <- recentScoreMovies) {
      print("==")
      // 计算此电影的相似电影与近期投票电影的相似度
      val sim = RealTimeRecommenderDemo.getMovieSim(top, recentScore._1)

      // 相似度小于0.3的相似电影会被忽略
      if (sim > 0.3){
        // 计算相似电影的优先级优先级
        prioritys +=  ((top, sim * recentScore._2.formatted("%.2f").toDouble))
        // 判断近期电影的投票，大于三增加增强因子，小于三增加减弱因子
        if (recentScore._2 > 3){
          in(top) = in.getOrDefault(top, 0) + 1
          de(top) = de.getOrDefault(top, 0)
        }
        else{
          de(top) = de.getOrDefault(top, 0) + 1
          in(top) = in.getOrDefault(top, 0)
        }
      }

    }

    println("compute")
    prioritys
      .groupBy(_._1)
      .map{case (movieId, priority) =>
        (
          movieId,
          priority.map(_._2).sum / priority.length
            + RealTimeRecommenderDemo.log(in(movieId))
            - RealTimeRecommenderDemo.log(de(movieId))
        )
      }
      .toArray
      .foreach(x => println(x._1 + ":" + x._2))

    print("close")

    MysqlUtil.close()

  }

}
