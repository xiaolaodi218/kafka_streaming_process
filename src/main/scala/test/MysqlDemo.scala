package test

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.ConnectionPool

import scala.collection.mutable.Queue

/**
  * Created by LIUWEI946 on 2017/5/31.
  */
object MysqlDemo {
  def main(args: Array[String]) {

    //创建spark实例
    val sparkConf = new SparkConf().setAppName("QueueStream")
    sparkConf.setMaster("local[2]")
    // 创建sparkStreamingContext ，Seconds是多久去Rdd中取一次数据。
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // Create the queue through which RDDs can be pushed to a QueueInputDStream
    var rddQueue = new Queue[RDD[String]]()
    // 从rdd队列中读取输入流
    val inputStream = ssc.queueStream(rddQueue)
    //将输入流中的每个元素（每个元素都是一个String）后面添加一个“a“字符，并返回一个新的rdd。
    val mappedStream = inputStream.map(x => (x, 1))
    //reduceByKey(_ + _)对每个元素统计次数。map(x => (x._2,x._1))是将map的key和value 交换位置。后边是过滤次数超过1次的且String 相等于“testa“
    val reducedStream = mappedStream.reduceByKey(_ + _)
      .map(x => (x._2, x._1))
    //.map(x => (x._2,x._1)).filter((x)=>x._1>1).filter((x)=>x._2.equals("test"))
    print("streaming output:")
    reducedStream.print()
    //将每次计算的结果存储在./out/resulted处。
    reducedStream.saveAsTextFiles("./out/resulted")

    //----------------------------------------------------------------
    //插入mysql数据库

    reducedStream.foreachRDD(rdd => rdd.foreachPartition(
      data => {
        val conn = ConnectionPool.getConnection.orNull
        //        val conn = ConnectPool.getConn("root", "1714004716", "h15", "dg")
        //插入数据
        //conn.prepareStatement("insert into t_word2(word,num) values('tom',23)").executeUpdate()
        try {
          for (row <- data) {
            println("input data is " + row._2 + "  " + row._1)
            val sql = "insert into result(name) values( '"+row._2  +"')"
            conn.prepareStatement(sql).executeUpdate()
          }
        } finally {
          conn.close()
        }
      }))
    //----------------------------------------------------------------
    ssc.start()

    //从数据库中查出每个用户的姓名，返回的是一个String有序队列seq，因为生成RDD的对象必须是seq。
    while (true) {
      val seq = conn1()
      rddQueue += ssc.sparkContext.makeRDD(seq, 10)
      /*      rddQueue.synchronized {
              rddQueue += ssc.sparkContext.makeRDD(seq, 10)
            }*/
      Thread.sleep(3000)
    }
    ssc.stop()
  }


  def conn1(): Seq[String] = {
    val conn = ConnectionPool.getConnection.orNull

    val sqlQuery="select * from test1 t where update_time> (SELECT update_time FROM compare_time ORDER BY id desc LIMIT 1)"
    val sqlUpdateTime="insert into compare_time(update_time) (select t.update_time from test1 t where t.update_time >= (SELECT update_time FROM compare_time ORDER BY id desc LIMIT 1) ORDER BY t.update_time desc LIMIT 1)"
    val seq = conn.prepareStatement(sqlQuery)
    var setName = Seq[String]()
    //    var setName = Seq("")
    try {
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val rs = statement.executeQuery(sqlQuery)
      while (rs.next) {
        // 返回行号
        // println(rs.getRow)
        val name = rs.getString("name")
        setName = setName :+ name
      }
      if(setName.length>0)
      {
        conn.prepareStatement(sqlUpdateTime).executeUpdate(sqlUpdateTime)
      }
      println("mysqlResult:" + setName)
      return setName
    } finally {
      conn.close
    }
  }

}
