package com.esni.recommendcommon.dao

import java.util.Properties

import com.esni.recommendcommon.common.RecommenderDao
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


trait DataFrameDao extends RecommenderDao{

  def readDataFrame(session: SparkSession, tableName: String, properties: Properties): DataFrame = {

    session
      .read
      .jdbc(properties.getProperty("mysql.uri"), tableName, properties)

  }

  def writeDataFrames(df: DataFrame, tableName: String, properties: Properties, saveMode: SaveMode=SaveMode.Overwrite): Unit = {

    df
      .write
      .mode(saveMode)
      .jdbc(properties.getProperty("mysql.uri"), tableName, properties)

  }

}
