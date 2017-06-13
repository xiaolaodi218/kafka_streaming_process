package test

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.ConnectionPool

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.io.Source

/**
  * Created by LIUWEI946 on 2017/6/5.
  */
object KafkaDstreamTest1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TestKafka").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val brokers = "jtcrtvdra104:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "smallest"//"group.id" -> "lww2017-06-05"//,"enable.auto.commit" -> "false"
    )
    var fx_shops: mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()

    val fxShops = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("source-mysql-hrtds-fx_shops"))
    fxShops.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(partitionOfRecords => {
        //Obtaining Offsets获取偏移量 lw-lw1 0 17 17
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        partitionOfRecords.foreach(oneStream => {
          var shop_id,merchant_code = ""
          var oneRecode: Array[Array[String]] = oneStream._2.substring(7, oneStream._2.length - 1).split(",").map(x => (x.split("=")))
          //println("record: " + oneStream._2.substring(7, oneStream._2.length - 1))
          for (items: Array[String] <- oneRecode) {
            if (items.length > 1 && items(0).toString == "shop_id") {
              shop_id = items(1).toString
            }
            if (items.length > 1 && items(0).toString == "merchantCode") {
              merchant_code = items(1).toString
            }
          }
          fx_shops += (shop_id -> merchant_code)
          println(shop_id,merchant_code)
        })
      })
    })
    //val broadcastVar = ssc.sparkContext.broadcast(fx_shops)
    //打印共享变量
    for ((shop_id, mechancode) <- fx_shops) {
      println(shop_id + ":" + mechancode)
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
