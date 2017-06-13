package com.crc.bigdata

import java.sql.Connection

import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import org.slf4j.LoggerFactory

/**
  * Created by zhanglei852 on 2017/5/23.
  */
object JdbcConnectionPool {
  val logger = LoggerFactory.getLogger(this.getClass)

  //连接池配置
  private val connectionPool: Option[BoneCP] = {
    val conf = new CommonConf()
    val db_connection = "mysql_hrtdsgo"
    val db_connection_db_type = conf.getDBConnectionProperty(db_connection,"db_type")
    val db_connection_url = conf.getDBConnectionProperty(db_connection,"url")
    val db_connection_user = conf.getDBConnectionProperty(db_connection,"user")
    val db_connection_password = conf.getDBConnectionProperty(db_connection,"password")

    db_connection_db_type match {
      case "mysql" => Class.forName("com.mysql.jdbc.Driver")
      case _ => throw new Exception("Not supported db type [" + db_connection_db_type + "] now.")
    }
    Class.forName(db_connection_db_type)
    val config = new BoneCPConfig()
//    config.setJdbcUrl("jdbc:mysql://10.0.108.151:3307/hrtdsgo?useUnicode=true&characterEncoding=UTF-8")
//    config.setUsername("bigdata")
//    config.setPassword("Bigdata@9876")
/*    config.setJdbcUrl("jdbc:mysql://10.0.108.33:3306/report?useUnicode=true&characterEncoding=UTF-8")
    config.setUsername("root")
    config.setPassword("Rep@@1228")*/
    config.setJdbcUrl(db_connection_url)
    config.setUsername(db_connection_user)
    config.setPassword(db_connection_password)
/*    config.setJdbcUrl("jdbc:mysql://localhost:3306/hrtest?useUnicode=true&characterEncoding=UTF-8")
    config.setUsername("root")
    config.setPassword("root")*/
    config.setLazyInit(true)

    config.setMinConnectionsPerPartition(3)
    config.setMaxConnectionsPerPartition(5)
    config.setPartitionCount(5)
    config.setCloseConnectionWatch(true)
    config.setLogStatementsEnabled(false)
    Some(new BoneCP(config))
  }

  // 获取数据库连接
  def getConnection: Option[Connection] = {
    connectionPool match {
      case Some(pool) => Some(pool.getConnection)
      case None => None
    }
  }

  // 释放数据库连接
  def closeConnection(connection:Connection): Unit = {
    if (!connection.isClosed) {
      connection.close()
    }
  }
}
