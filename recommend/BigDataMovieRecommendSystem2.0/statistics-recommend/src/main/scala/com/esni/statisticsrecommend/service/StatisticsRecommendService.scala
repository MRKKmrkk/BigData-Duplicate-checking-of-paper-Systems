package com.esni.statisticsrecommend.service

import com.esni.recommendcommon.common.RecommenderService
import com.esni.statisticsrecommend.dao.TableDao

class StatisticsRecommendService extends RecommenderService{

  private val tableDao = new TableDao

  /**
    * 计算历史热门电影
    * 统计所有历史数据中每个电影的评分数
    * 并存入电影评分个数统计表
    */
  private def computePopularHistoricalMovies(): Unit = {

    tableDao.saveQueryResultAsTable("movie_score_count",
      """
        |select
        |	a.movie_id as movie_id,
        |	if (b.score_count is null, 0, b.score_count) as score_count
        |from movie_info as a
        |left join (
        |	select
        |		movieId,
        |		count(*) as score_count
        |	from movie_score
        |	group by movieId
        |) as b
        |on a.movie_id = b.movieId
      """.stripMargin)

  }

  /**
    * 计算近期热门电影
    * 统计每个月的电影评分个数
    * 存入最近电影评分表
    */
  def computeRecentPopularMovies(): Unit = {

    tableDao.saveQueryResultAsTable("movie_recent_score_count",
      """
        |select
        |	b.movie_id,
        |	if (c.score_count is null, 0, c.score_count) as score_count,
        |	if (c.year_month is null, '0000-00', c.year_month) as year_month
        |from movie_info as b
        |left join (
        |	select
        |		movieId as movie_id,
        |		count(*) as score_count,
        |		year_month
        |	from (
        |		select
        |			movieId,
        |			from_unixtime(cast(ts/1000 as int), 'yyyy-MM') as year_month
        |		from movie_score
        |	) as a
        |	group by movieId,year_month
        |) as c
        |on b.movie_id = c.movie_id
      """.stripMargin)

  }

  def computeAvgMovieScore(): Unit = {

    val resultDF = tableDao.getDataFrame(
      """
        |select
        |	d.movie_id,
        |	if (avg_score is null, 0, avg_score) as avg_score
        |from movie_info as d
        |left join (
        |	select
        |		c.movieId,
        |		cast(avg(c.score) as decimal(10, 2)) as avg_score
        |	from (
        |		select
        |			a.movieId,
        |			a.score
        |		from movie_score as a join (
        |			select
        |				userId,
        |				movieId,
        |				max(ts) as mts
        |			from movie_score
        |			group by userId, movieId
        |		) as b on a.userId=b.userId and a.movieId=b.movieId and a.ts=b.mts
        |	) as c
        |	group by c.movieId
        |) as e
        |on d.movie_id = e.movieId
      """.stripMargin)
    tableDao.createTempleView("movie_avg_score", resultDF)
    tableDao.saveDataFrame(resultDF, "movie_avg_score")

  }

  def computeAllCategoriesTop10AvgScore(): Unit = {

    tableDao.saveQueryResultAsTable("movie_category_top10",
      """
        |select
        |	e.category_id,
        |	e.ml[0] as top1_movie_id,
        |	e.ml[1] as top2_movie_id,
        |	e.ml[2] as top3_movie_id,
        |	e.ml[3] as top4_movie_id,
        |	e.ml[4] as top5_movie_id,
        |	e.ml[5] as top6_movie_id,
        |	e.ml[6] as top7_movie_id,
        |	e.ml[7] as top8_movie_id,
        |	e.ml[8] as top9_movie_id,
        |	e.ml[9] as top10_movie_id
        |from (
        |	select
        |		d.category_id,
        |		collect_list(d.movie_id) as ml
        |	from (
        |		select
        |			c.category_id,
        |			c.movie_id,
        |			row_number() over(partition by c.category_id order by c.avg_score desc) as rk
        |		from (
        |			select
        |				b.category_id,
        |				a.movie_id,
        |				a.avg_score
        |			from movie_avg_score as a
        |			join movie_category as b
        |			on a.movie_id = b.movie_id
        |		) as c
        |	) as d where d.rk <= 10
        |	group by d.category_id
        |) as e
      """.stripMargin)

  }

  override def execute(): Unit = {

    tableDao.createTables()

    computePopularHistoricalMovies()
    computeRecentPopularMovies()
    computeAvgMovieScore()
    computeAllCategoriesTop10AvgScore()

  }

}
