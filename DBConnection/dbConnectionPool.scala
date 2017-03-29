package com.rs.DBConnection

import java.sql.Connection
import java.sql.DriverManager
 
import com.jolbox.bonecp.BoneCP
import com.jolbox.bonecp.BoneCPConfig

object dbConnectionPool {
	  
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://localhost/EntityResolution"
//    val url = "jdbc:mysql://52.87.234.47:22/entityresolution"
  val user = "root"
  val pass = "password"
    
  private val connectionPool = {
    try {
      Class.forName(driver)
      
      val config = new BoneCPConfig()
      
      config.setJdbcUrl(url)
      config.setUsername(user)
      config.setPassword(pass)
      config.setMinConnectionsPerPartition(2)
      config.setMaxConnectionsPerPartition(5)
      config.setPartitionCount(1)
      config.setCloseConnectionWatch(true)// if connection is not closed throw exception
      config.setLogStatementsEnabled(true) // for debugging purpose
      
      Some(new BoneCP(config))
    } 
    catch {
      case e => e.printStackTrace()
      None
    }
  }

  def getConnection: Option[Connection] = {
    connectionPool match {
      case Some(connPool) => Some(connPool.getConnection)
      case None => None
    }
  }
}