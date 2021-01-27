package com.esni.statisticsrecommend.dao

import java.util.Properties

import com.esni.recommendcommon.dao.{DataFrameDao, RddDAO}
import com.esni.recommendcommon.util.{EnvironmentUtil, PropertiesUtil}
import com.esni.statisticsrecommend.bean.MovieScore
import org.apache.spark.sql.{DataFrame, SparkSession}

class TableDao extends DataFrameDao with RddDAO{

  private val properties: Properties = PropertiesUtil.getProperties("database.properties")
  private val session: SparkSession = EnvironmentUtil.getSparkSession

  def createTempleView(viewName: String, df: DataFrame): Unit = {

    df.createTempView(viewName)

  }

  def createTables(): Unit = {

    createTempleView("movie_category", readDataFrame(session, "movie_category", properties))

    createTempleView("movie_info", readDataFrame(session, "movie_info", properties))

    import session.implicits._
    createTempleView("movie_score",
      readScoreRdd(EnvironmentUtil.getSparkContext, EnvironmentUtil.getArguments()(0))
      .map(fields => MovieScore(fields._1, fields._2, fields._3, fields._4))
      .toDF
    )

  }

  def getDataFrame(sql: String): DataFrame = {

    session
      .sql(sql)

  }

  def saveDataFrame(df: DataFrame, tableName: String): Unit = {

    writeDataFrames(df, tableName, properties)

  }

  def saveQueryResultAsTable(tableName: String, sql: String): Unit = {

    writeDataFrames(getDataFrame(sql), tableName, properties)

  }

}
