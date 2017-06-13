package com.crc.bigdata.hrtdsgo

import com.crc.bigdata.CommonConf
import kafka.serializer.StringDecoder
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}

import scala.collection.mutable
import scala.util.Try

/**
  * Created by zhanglei852 on 2017/6/12.
  */
object FxShops {
  val conf = new CommonConf()
  val kafka_conf_name="kafka_confluent_3.2.1"
  val brokers=conf.getKafkaProperty(kafka_conf_name,"brokers")

  def broadcast(ssc: StreamingContext, broadcastFxShops: Broadcast[mutable.Map[String, String]]): Unit = {
/*    val brokers = "jtcrtvpra304:9092";
    "jtcrtvpra305:9092";
    "jtcrtvpra306:9092"*/

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "largest",
      "group.id" -> "aaaa"
      //,"enable.auto.commit" -> "false"
    )
    val fxShops = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("source-mysql-hrtds-fx_shops"))
    fxShops.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(partitionOfRecords => {
        //Obtaining Offsets获取偏移量 lw-lw1 0 17 17

        partitionOfRecords.foreach(oneStream => {
          val oneRecode: Map[String,String] = oneStream._2.substring(7, oneStream._2.length - 1).split(",").map(_.split("=",2)).map(x => (x(0),Try(x(1)).getOrElse(""))).toMap
          val shop_id = oneRecode.getOrElse("shop_id","")
          val merchant_code = oneRecode.getOrElse("merchantCode","unknow")
          if (shop_id != "") {
            broadcastFxShops.value.put(shop_id, merchant_code)
          }
//          println("shop_id", shop_id, "merchant_code", merchant_code)
        })
      })
    })
  }
}
