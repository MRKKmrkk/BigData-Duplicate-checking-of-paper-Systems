package com.esni.realtimerecommend.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object MysqlUtil{

  private val connection: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/train", "root", "mailbox330.")
  private val preSatement: PreparedStatement = connection.prepareStatement("select movie_matrix from movie_sim_matrix where movie_id = ?")

  // 初始化表
  private val initPreparedStatement: PreparedStatement = connection.prepareStatement("CREATE TABLE IF NOT EXISTS movie_realtime_recommend(user_id INT, movie_1 INT, movie_2 INT, movie_3 INT, movie_4 INT, movie_5 INT, movie_6 INT, movie_7 INT, movie_8 INT, movie_9 INT, movie_10 INT)")
  initPreparedStatement.execute()
  initPreparedStatement.close()


  def getMovieMatrixAsArray(id: Int): Array[(Int, Double)] = {

    preSatement.setInt(1, id)
    val rs: ResultSet = preSatement.executeQuery()

    if (rs.next()) {
      rs
        .getString(1)
        .split(" ")
        .map{
          item =>
            val fields = item.split(":")
            (fields(0).toInt, fields(1).toDouble)
        }
    }
    else{
      new Array[(Int, Double)](0)
    }

  }

  def getMovieMatrixAsMap(id: Int): Map[Int, Double] = {

    val matrix = getMovieMatrixAsArray(id)

    if (matrix.length > 0) {
      matrix
        .toMap
    }
    else{
      Map()
    }
  }

  def saveRealTimeMovieRecommend(userId: Int, recommendMovies: Array[(Int, Double)]): Unit = {

    val savePreSatement: PreparedStatement = connection.prepareStatement("insert into movie_realtime_recommend() values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

    val arrLength = recommendMovies.length
    savePreSatement.setInt(1, userId)

    for (i <- 0 to 9){
      if (i < arrLength){
        savePreSatement.setInt(i + 2, recommendMovies(i)._1)
      }
      else{
        savePreSatement.setInt(i + 2, -1)
      }
    }

    savePreSatement.execute()
    savePreSatement.close()

  }

  def close(): Unit = {

    preSatement.close()
    connection.close()

  }

}
