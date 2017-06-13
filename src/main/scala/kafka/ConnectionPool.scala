package kafka

import java.sql.Connection

import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import org.slf4j.LoggerFactory

/**
  * Created by zhanglei852 on 2017/5/23.
  */
object ConnectionPool {
  val logger = LoggerFactory.getLogger(this.getClass)

  //连接池配置
  private val connectionPool: Option[BoneCP] = {
    Class.forName("com.mysql.jdbc.Driver")
    val config = new BoneCPConfig()
    config.setJdbcUrl("jdbc:mysql://10.0.53.81:3306/kafka?useUnicode=true&characterEncoding=UTF-8")
    config.setUsername("root")
    config.setPassword("Big@2016")
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
