package com.esni.statisticsrecommender

import java.io.InputStream
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

case class Score(userId: Int, movieId: Int, score: Int, ts: Long)


object StatisticsRecommender {

  val conf: SparkConf = new SparkConf().setAppName("Statistics Recommender").setMaster("local[*]")
  val sc: SparkContext = new SparkContext(conf)
  val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  val properties = new Properties()
  private val loader: ClassLoader = Thread.currentThread().getContextClassLoader
  private val stream: InputStream = loader.getResourceAsStream("database.properties")
  properties.load(stream)

  /**
    * 将结果存入Mysql
    * @param df：要存入Mysql的DF
    * @param tableName： 存入的表
    * @param saveMode: 存储类型，默认覆盖原表
    */
  def saveInMysql(df: DataFrame, tableName: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {

    df.write.mode(saveMode).jdbc(properties.getProperty("uri"), tableName, properties)

  }

  /**
    * 计算历史热门电影
    * 统计所有历史数据中每个电影的评分数
    * 并存入电影评分个数统计表
    */
  def saveMovieScoreCount(): Unit = {

    val resultDF = session.sql("select movieId, count(*) as count from movie_score group by movieId")
    saveInMysql(resultDF, "movie_score_count")

  }

  /**
    * 计算近期热门电影
    * 统计每个月的电影评分个数
    * 存入最近电影评分表
    */
  def saveMovieScoreRecentCount(): Unit = {

    val resultDF = session.sql("select movieId,count(*) as count, year_month from (select movieId,from_unixtime(cast(ts/1000 as int), 'yyyy-MM') as year_month from movie_score) as a group by movieId,year_month")

    saveInMysql(resultDF, "movie_recent_score_count")

  }

  def saveMovieAvgScore(): Unit = {

    val resultDF = session.sql("select c.movieId, cast(avg(c.score) as decimal(10, 2)) as avg_score from (select a.movieId, a.score from movie_score as a join (select userId, movieId, max(ts) as mts from movie_score group by userId, movieId) as b on a.userId=b.userId and a.movieId=b.movieId and a.ts=b.mts) as c group by c.movieId")
    saveInMysql(resultDF, "movie_avg_score")

  }

  def main(args: Array[String]): Unit = {

    //408	4566	2	1611039929405
    val ratingsRdd = sc.textFile("D:\\Projects\\dpystem\\recommend\\testData\\ur.log")
      .map(_.split("\t"))
      .map(fields => (fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3).toLong))

    val behaviorsRdd = sc.textFile("D:\\Projects\\dpystem\\recommend\\testData\\ub.log")
        .map(_.split("\t"))
        .map{case Array(uid, mid, behavior, ts) =>
          var weight = 0
          if ("0".equals(behavior)) {
            weight = 1
          }
          if ("1".equals(behavior)) {
            weight = 2
          }
          if ("2".equals(behavior)) {
            weight = 2
          }
          if ("3".equals(behavior)) {
            weight = -2
          }
          (uid, mid, weight, ts)
        }

    import session.implicits._
    val ratingsDF = ratingsRdd.toDF("userId", "movieId", "score", "ts").as[Score]
    ratingsDF.createTempView("movie_score")

    // 从Mysql读取电影分类数据,并创建表
    val categoryDF = session.read.jdbc(properties.getProperty("uri"), "movie_category", properties)
    categoryDF.createTempView("movie_category")

    saveMovieAvgScore()

    //关闭回收资源
    session.close()
    sc.stop()

  }

}
