package com.esni.realtimerecommend.util

import java.sql.{Connection, DriverManager}

object ConnectionHelper{

  def getConnection() : Connection = {

    DriverManager.getConnection("jdbc:mysql://localhost:3306/train", "root", "mailbox330.")

  }

}
