package test

import java.sql.{Date, ResultSet}

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.ConnectionPool

import scala.collection.mutable.Queue

/**
  * Created by LIUWEI946 on 2017/5/31.
  */
object test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("QueueStream")setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    var rddQueue = new Queue[RDD[String]]()
    val inputStream = ssc.queueStream(rddQueue)
    val mappedStream = inputStream.map(x => (x, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
      .map(x => (x._2, x._1))
    print("streaming output:")
    reducedStream.print()
    reducedStream.saveAsTextFiles("./out/resulted")

    ssc.start()
    while (true) {
      val seq = conn()
      rddQueue += ssc.sparkContext.makeRDD(seq, 10)
      Thread.sleep(3000)
    }
    ssc.stop()
  }
  def conn():Seq[String]= {
    val conn = ConnectionPool.getConnection.orNull
    val sql = "select * from test1"
    //val seq = conn.prepareStatement(sql)
    var setName = Seq[String]()
    try {
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val rs = statement.executeQuery(sql)
      while (rs.next) {
        // 返回行号
        // println(rs.getRow)
        val name = rs.getString("name")
        setName = setName :+ name
      }
      println("mysqlResult:" + setName)
      return setName


    } finally {
      ConnectionPool.closeConnection(conn)
    }
  }
}
