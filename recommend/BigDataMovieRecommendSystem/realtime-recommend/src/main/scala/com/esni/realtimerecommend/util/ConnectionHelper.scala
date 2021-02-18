package com.esni.realtimerecommend.util

import java.sql.{Connection, DriverManager}

object ConnectionHelper extends Serializable {

  /**
    * 返回连接对象
    */
  def getConnection() : Connection = {

    DriverManager.getConnection("jdbc:mysql://localhost:3306/train", "root", "mailbox330.")

  }

}
