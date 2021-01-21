package com.esni.statisticsrecommender

import java.io.InputStream
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

case class Score(userId: Int, movieId: Int, score: Int, ts: Long)

/**
  * 存在的问题
  * 1.Mysql存储时覆盖是否会导致问题
  * 2.变量名字段名修改
  * 3.spark sql语句的缩进
  * 4.是否创建sql工具类，以及rdd的处理类
  */

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

  /**
    * 电影平均分统计
    * 存入电影平均评分分表
    * 创建表未接下来的分类前十分析使用
    */
  def saveMovieAvgScore(): Unit = {

    val resultDF = session.sql("select c.movieId, cast(avg(c.score) as decimal(10, 2)) as avg_score from (select a.movieId, a.score from movie_score as a join (select userId, movieId, max(ts) as mts from movie_score group by userId, movieId) as b on a.userId=b.userId and a.movieId=b.movieId and a.ts=b.mts) as c group by c.movieId")
    //创建表未接下来的分类前十分析使用
    resultDF.createTempView("movie_avg_score")
    saveInMysql(resultDF, "movie_avg_score")

  }

  /**
    * 各类别top10评分电影统计
    * 存入movie_category_top10表
    * 执行此方法之前需要先执行saveMovieAvgScore方法
    */
  def saveMovieCategoryTop10(): Unit = {

    val resultDF = session.sql("select e.categoryId as category_id, e.ml[0] as top1_movie_id, e.ml[1] as top2_movie_id, e.ml[2] as top3_movie_id, e.ml[3] as top4_movie_id, e.ml[4] as top5_movie_id, e.ml[5] as top6_movie_id, e.ml[6] as top7_movie_id, e.ml[7] as top8_movie_id, e.ml[8] as top9_movie_id, e.ml[9] as top10_movie_id from (select d.categoryId, collect_list(d.movieId) as ml from (select c.categoryId, c.movieId, row_number() over(partition by c.categoryId order by c.avg_score desc) as rk from (select b.categoryId, a.movieId, a.avg_score from movie_avg_score as a join movie_category as b on a.movieId = b.movieId) as c) as d where d.rk <= 10 group by d.categoryId) as e")
    saveInMysql(resultDF, "movie_category_top10")

  }

  def createMovieRatingTable(path: String): Unit = {

    val ratingsRdd = sc.textFile(path)
      .map(_.split("\t"))
      .map(fields => (fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3).toLong))

    import session.implicits._
    ratingsRdd.toDF("userId", "movieId", "score", "ts").as[Score]
      .createTempView("movie_score")

  }

  def createMovieCategoryTable(): Unit = {

    session
      .read
      .jdbc(properties.getProperty("uri"), "movie_category", properties)
      .createTempView("movie_category")

  }

  def main(args: Array[String]): Unit = {

    // 导入rating数据，并建表
    createMovieRatingTable("D:\\Projects\\dpystem\\recommend\\testData\\ur.log")

    // 从Mysql读取电影分类数据,并创建表
    createMovieCategoryTable()

    // 开始统计分析，并将结果存入数据库
    saveMovieScoreCount()
    saveMovieScoreRecentCount()
    saveMovieAvgScore()
    saveMovieCategoryTop10()

    //关闭回收资源
    session.close()
    sc.stop()

  }

}
