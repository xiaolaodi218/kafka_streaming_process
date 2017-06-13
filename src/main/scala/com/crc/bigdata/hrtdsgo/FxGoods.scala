package com.crc.bigdata.hrtdsgo

import com.crc.bigdata.CommonConf
import kafka.serializer.StringDecoder
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable
import scala.util.Try

/**
  * Created by Administrator on 2017/6/12.
  */
object FxGoods {
  def broadcast(ssc: StreamingContext, broadcastFxShops: Broadcast[mutable.Map[String, String]]): Unit = {
    val brokers=new CommonConf().getKafkaProperty("kafka_confluent_3.2.1","brokers")
//
//    val brokers ="jtcrtvpra304:9092";
//    "jtcrtvpra305:9092";
//    "jtcrtvpra306:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "largest", ///lly test modify
      "group.id" -> "lww2017-06-05"
      //,"enable.auto.commit" -> "false"
    )
    val fxGoods = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("source-mysql-hrtds-fx_goods"))
    fxGoods.foreachRDD(rdd => {
      // val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(partitionOfRecords => {
        //Obtaining Offsets获取偏移量 lw-lw1 0 17 17
        // val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        // println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        partitionOfRecords.foreach(oneStream => {
          //oneStream._2.substring(7, oneStream._2.length - 1)待修改
          //var oneRecode: Array[Array[String]] = oneStream._2.substring(7, oneStream._2.length - 1).split(",").map(x => (x.split("=")))
          val oneRecode: Map[String,String] = oneStream._2.substring(7, oneStream._2.length - 1).split(",").map(_.split("=",2)).map(x => (x(0),Try(x(1)).getOrElse(""))).toMap
          val g_id = oneRecode.getOrElse("g_id","")
          val shop_id = oneRecode.getOrElse("shop_id","")

          if (g_id != "") {
            broadcastFxShops.value.put(g_id, shop_id)
          }
//          println("g_id",g_id,"shop_id", shop_id)

        })
      })
    })
  }
}
