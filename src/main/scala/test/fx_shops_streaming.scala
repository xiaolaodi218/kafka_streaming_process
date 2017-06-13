package test

import kafka.serializer.StringDecoder
import org.apache.spark.TaskContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

import scala.collection.mutable

/**
  * Created by LIUWEI946 on 2017/6/7.
  */
object fx_shops_streaming {

  def fetchStreaming(ssc:StreamingContext, kafkaParams:Map[String, String]):mutable.Map[String, String]={
    val fxShops = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("source-mysql-hrtds-fx_shops"))
    //每天凌晨清空rdd
    /*   lines.foreachRDD(r => {
         r.foreach(s => {
           //println(s._1)
           println(s._2) //Struct{id=3,name=华润万家,dt=2017-06-02 17:43:17.0}

         })
       })*/
    var fx_shops: mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()
    fxShops.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(partitionOfRecords => {
        //Obtaining Offsets获取偏移量 lw-lw1 0 17 17
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        partitionOfRecords.foreach(oneStream => {
          var shop_id,merchant_code=""
          var oneRecode: Array[Array[String]] = oneStream._2.substring(7, oneStream._2.length - 1).split(",").map(x => (x.split("=")))
          for (items: Array[String] <- oneRecode) {
            if (items.length > 1 && items(0).toString=="shop_id") {
              shop_id=items(1).toString
            }
            if (items.length > 1 && items(0).toString=="merchantCode") {
              merchant_code=items(1).toString
            }
          }
          fx_shops += (shop_id -> merchant_code)
        })
      })
    })
    val broadcastVar = ssc.sparkContext.broadcast(fx_shops)
    broadcastVar.value
  }
}
