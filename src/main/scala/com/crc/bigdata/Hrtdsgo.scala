package com.crc.bigdata

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import kafka.serializer.StringDecoder
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

import scala.collection.mutable
import scala.util.Try

/**
  * Created by LIUWEI946 on 2017/5/31.
  */
object Hrtdsgo extends App {

  override def main(args: Array[String]): Unit = {

    val conf = new CommonConf()
    val spark_conf_name = "spark_cdh_5.11"
    val master = conf.getSparkProperty(spark_conf_name,"host_and_port")
    val kafka_conf_name="kafka_confluent_3.2.1"
    val brokers=conf.getKafkaProperty(kafka_conf_name,"brokers")

    val fx_shops = mutable.Map[String, String]()
    val fx_orders = mutable.Map[String, String]()
    val fx_orders_refunds = mutable.Map[String, String]()
    val fx_goods = mutable.Map[String, String]()
    val deviceInfo = mutable.Map[String, String]()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val strFormat1 = new SimpleDateFormat("yyyyMMdd")
    //    val sparkConf = new SparkConf().setAppName("TestKafka").setMaster("spark://10.0.108.234:7077")
    //    val sparkConf = new SparkConf().setAppName("TestKafka").setMaster("spark://10.0.53.109:7077")
//    val sparkConf = new SparkConf().setAppName("TestKafka").setMaster("local[1]").setJars(Array("D:\\IdeaProjects\\alphago\\target\\artifacts\\alphago_jar\\alphago.jar"))
    val sparkConf = new SparkConf().setAppName("hrtdsgo").setMaster(master)
      //.setJars(Array("D:\\IdeaProjects\\alphago\\target\\artifacts\\alphago_jar\\alphago.jar"))
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    var broadcastFxShops = ssc.sparkContext.broadcast(fx_shops)
    var broadcastFxOrders = ssc.sparkContext.broadcast(fx_orders)
    val broadcastFxGoods = ssc.sparkContext.broadcast(fx_goods)
    val broadcastDeviceInfo = ssc.sparkContext.broadcast(deviceInfo)
    hrtdsgo.FxShops.broadcast(ssc, broadcastFxShops)
    hrtdsgo.FxGoods.broadcast(ssc, broadcastFxGoods)

/*    val brokers = "jtcrtvpra304:9092";
    "jtcrtvpra305:9092";
    "jtcrtvpra306:9092"*/

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "largest",
      "group.id" -> "lww2017-06-05" //,"enable.auto.commit" -> "false"
    )

    val topics = Set("source-mysql-hrtds-fx_orders", "source-mysql-hrtds-fx_orders_refunds")
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    lines.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      var currentTopic = ""
      rdd.foreachPartition(partitionOfRecords => {
        //Obtaining Offsets获取偏移量 lw-lw1 0 17 17
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        currentTopic = s"${o.topic}"

        partitionOfRecords.foreach(oneRecode => {
          //println("record: " + oneRecode._2)
          //一条一条记录处理逻辑
          process(currentTopic, oneRecode._2, broadcastFxShops, broadcastFxOrders, fx_orders_refunds) //broadcastVar.value
          //, fx_shops, fx_orders, fx_shops
        })
      })
      // some time later, after outputs have completed 提交偏移量
      //lines.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })
    val kafkaStream_app = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("topic_app"))


    kafkaStream_app.foreachRDD(r => {
      r.foreachPartition(p => {
        p.foreach(f => {
          try{
            val a = f._2.trim().substring(f._2.trim().indexOf("{"))
            val json = JSON.parseObject(a)
            val deviceId = json.getJSONObject("deviceInfo").getString("id")
            val regex ="""good_[0-9]""".r
            val ei = json.getJSONArray("eventsInfo")
            ei.toArray().foreach(eii => {
              eii match {
                case m: JSONObject => {
                  val sysType = m.getString("type")
                  val sysTime = scala.util.Try(strFormat1.format(dateFormat.parse(m.getString("time")))).getOrElse("2001-01-01")
                  val pageName = m.getString("pageName")
                  val shopDetail = m.getString("shopDetail")
                  var shop_id = ""

                  if ("page".equals(sysType) && strFormat1.format(new Date()).equals(sysTime)) {

                    println(deviceId, sysTime, shopDetail, pageName)

                    if (shopDetail != null && shopDetail.length > 0) {
                      shop_id = pageName.substring(11)
                      executePV(sysTime, shop_id, broadcastFxShops) ///修改对应字段
                      println("executePV===============================")
                    } else if ((regex findAllIn pageName isEmpty) != true) {
                      var g_id = pageName.substring(5)
                      shop_id = broadcastFxGoods.value.getOrElse(g_id, "unknow")
                      executePV(sysTime, shop_id, broadcastFxShops) //goods_id=34647
                      println("pv==============================================")
                    }

                    val x = broadcastDeviceInfo.value
                    val y = x.exists(Set(deviceId,shop_id))
                    if (shop_id != "" && !broadcastDeviceInfo.value.exists(_ == (deviceId,shop_id))) {
                      broadcastDeviceInfo.value.put(deviceId, shop_id)
                      executeUV(sysTime, shop_id, broadcastFxShops)
                    }
                  }
                }
              }
            })
          } catch {
            case e : Throwable => e.printStackTrace()
          }
        })
      })
    })





    ssc.start()
    ssc.awaitTermination()
  }

  def getNowDate(): String = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var today = dateFormat.format(now)
    today
  }

  def process(currentTopic: String, record: String, fx_shops: Broadcast[mutable.Map[String, String]], fx_orders: Broadcast[mutable.Map[String, String]], fx_orders_refunds: mutable.Map[String, String]): Unit = {
    //, fx_shops: mutable.Map[String, String], fx_orders: mutable.Map[String, String], fx_orders_refunds: mutable.Map[String, String]
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val yyyymmddFormat = new SimpleDateFormat("yyyyMMdd")
    //val statusSet = mutable.Set("WAIT_BUYER_PAY", "WAIT_SELLER_SEND_GOODS", "ORDER_COMPLETED") //需要处理的订单状态
    //  println(currentTopic + ":" + record)

    val resultsMap: Map[String, String] = record.substring(7, record.length - 1).split(",").map(_.split("=", 2)).map(x => (x(0), Try(x(1)).getOrElse(""))).toMap

    if (currentTopic == "source-mysql-hrtds-fx_orders") {
      val o_id = resultsMap.getOrElse("o_id", "")
      val shop_id = resultsMap.getOrElse("shop_id", "unknown")
      val order_create_time = yyyymmddFormat.format(dateFormat.parse(resultsMap.getOrElse("o_create_time", "2001-01-01")))
      val new_order_status = resultsMap.getOrElse("o_status", "")
      val o_pay = resultsMap.getOrElse("o_pay", "0")
      val o_point_consume = resultsMap.getOrElse("o_point_consume", "0")
      val merchant_code = fx_shops.value.getOrElse(shop_id, "unknown")

      //处理当天下单或状态发生变化的订单
      if (o_id != "" && order_create_time == yyyymmddFormat.format(new Date()) && !fx_orders.value.exists(_ == (o_id, new_order_status))) {//&& statusSet.contains(new_order_status)
        println(currentTopic + ":" + record)
        //新订单加入fx_orders：（订单id->订单status）
        val old_order_status = fx_orders.value.getOrElse(o_id, "")
        fx_orders.value.put(o_id, new_order_status)

        //插入记录
        val sqlInsert =
          """
            |insert into fx_day_statistics(merchant_code, shop_code, date_time)
            |select '$$merchant_code', '$$shop_code', '$$date_time'
            |from dual
            |where not exists (select 1 from fx_day_statistics where merchant_code ='$$merchant_code' and shop_code='$$shop_code' and date_time = '$$date_time')
          """.stripMargin
            .replace("$$merchant_code", merchant_code)
            .replace("$$shop_code", shop_id)
            .replace("$$date_time", order_create_time)

        println("merchant_code", merchant_code, " shop_id", shop_id, "date_time", order_create_time)
        println("sqlInsert", sqlInsert)
        executeSql(sqlInsert)

        var sqlSet1 = new_order_status match {
          case "WAIT_BUYER_PAY" => "wait_pay_order = wait_pay_order + 1"
          case "WAIT_SELLER_SEND_GOODS" => "wait_delivery_order = wait_delivery_order + 1"
          case "ORDER_COMPLETED" => "success_order = success_order + 1, transaction_amount = transaction_amount + " + o_pay + " , transaction_point = transaction_point + " + o_point_consume
          //case "CUSTOMER_SERVICE_PROCESSING" => "after_safe_order = after_safe_order + 1"
          case _ => "wait_pay_order = wait_pay_order "
        }
        val sqlSet2 = old_order_status match {
          case "WAIT_BUYER_PAY" => ",wait_pay_order = wait_pay_order - 1"
          case "WAIT_SELLER_SEND_GOODS" => ",wait_delivery_order = wait_delivery_order - 1"
          case "ORDER_COMPLETED" => ",success_order = success_order - 1, transaction_amount = transaction_amount - " + o_pay + " , transaction_point = transaction_point - " + o_point_consume
          //case "CUSTOMER_SERVICE_PROCESSING" => ",after_safe_order = after_safe_order - 1"
          case _ => ""
        }
        val sqlUpdate =
          """
            |UPDATE fx_day_statistics
            |SET $$set
            |WHERE merchant_code = '$$merchant_code' AND shop_code = '$$shop_code' AND date_time = '$$date_time'
          """.stripMargin
            .replace("$$merchant_code", merchant_code)
            .replace("$$shop_code", shop_id)
            .replace("$$date_time", order_create_time)
            .replace("$$set", sqlSet1 + sqlSet2)

        println("sqlUpdate:" + sqlUpdate)
        executeSql(sqlUpdate)
      }
    }
    else if (currentTopic == "source-mysql-hrtds-fx_orders_refunds") {
      val or_id = resultsMap.getOrElse("or_id", "")
      val shop_id = resultsMap.getOrElse("shop_id", "unknown")
      val or_create_time = yyyymmddFormat.format(dateFormat.parse(resultsMap.getOrElse("or_create_time", "2001-01-01")))
      val or_money = resultsMap.getOrElse("or_money", "0")
      val or_point_money = resultsMap.getOrElse("or_point_money", "")
      val new_order_status = resultsMap.getOrElse("or_processing_status", "")
      //"CUSTOMER_SERVICE_PROCESSING" => "after_safe_order = after_safe_order + 1"
      val merchant_code = fx_shops.value.getOrElse(shop_id, "unknown")
      val endStatusSet = mutable.Set("REFUND_CLOSED", "REFUND_CANCEL", "RETURN_CLOSED","REFUND_PROCESSING","RETURN_CANCEL") //这些状态的订单都都不在售后中

      //今天未处理过的退款订单
      if (or_id != "" && or_create_time == yyyymmddFormat.format(new Date()) && !fx_orders_refunds.contains(or_id)) {
        println("merchant_code", merchant_code, "shop_id", shop_id, "date_time", or_create_time)
        //新订单加入fx_orders：（订单id->订单status）
        fx_orders_refunds.put(or_id, new_order_status)

        //插入记录
        val sqlInsert =
          """
            |insert into fx_day_statistics(merchant_code, shop_code, date_time)
            |select '$$merchant_code', '$$shop_code', '$$date_time'
            |from dual
            |where not exists (select 1 from fx_day_statistics where merchant_code ='$$merchant_code' and shop_code='$$shop_code' and date_time = '$$date_time')
          """.stripMargin
            .replace("$$merchant_code", merchant_code)
            .replace("$$shop_code", shop_id)
            .replace("$$date_time", or_create_time)

        println("merchant_code:" + merchant_code + " shop_id:" + shop_id + " date_time:" + or_create_time)
        println("sqlInsert:" + sqlInsert)
        executeSql(sqlInsert)

        if(!endStatusSet.contains(new_order_status) ){
          var sqlUpdate =
            """
              |UPDATE fx_day_statistics
              |SET after_safe_order = after_safe_order + 1
              |WHERE  merchant_code = '$$merchant_code' AND shop_code = '$$shop_code' AND date_time = '$$date_time'
            """.stripMargin
              .replace("$$merchant_code", merchant_code)
              .replace("$$shop_code", shop_id)
              .replace("$$date_time", or_create_time)

          println("sqlUpdate:" + sqlUpdate)
          executeSql(sqlUpdate)
        }
        if (new_order_status == "REFUND_SUCCESS" || new_order_status == "RETURN_SUCCESS") {
          var sqlUpdate =
            """
              |UPDATE fx_day_statistics
              |SET after_safe_order = after_safe_order - 1
              |   ,refund_amount = refund_amount + $$refund_amount
              |   ,refund_point = refund_point + $$refund_point
              |WHERE  merchant_code = '$$merchant_code' AND shop_code = '$$shop_code' AND date_time = '$$date_time'
            """.stripMargin
              .replace("$$merchant_code", merchant_code)
              .replace("$$shop_code", shop_id)
              .replace("$$date_time", or_create_time)
              .replace("$$refund_amount", or_money)
              .replace("$$refund_point", or_point_money)

          println("sqlUpdate:" + sqlUpdate)
          executeSql(sqlUpdate)
        }

      }
      //今天已经加入fx_orders_refunds的退款单
      else if (or_id != "" && or_create_time == yyyymmddFormat.format(new Date()) && fx_orders_refunds.contains(or_id)) {
        //新订单加入fx_orders：（订单id->订单status）
        fx_orders_refunds.put(or_id, new_order_status)
        if (new_order_status == "REFUND_SUCCESS" || new_order_status == "RETURN_SUCCESS") {
          var sqlUpdate =
            """
              |UPDATE fx_day_statistics
              |SET after_safe_order = after_safe_order - 1
              |   ,refund_amount = refund_amount + $$refund_amount
              |   ,refund_point = refund_point + $$refund_point
              |WHERE  merchant_code = '$$merchant_code' AND shop_code = '$$shop_code' AND date_time = '$$date_time'
            """.stripMargin
              .replace("$$merchant_code", merchant_code)
              .replace("$$shop_code", shop_id)
              .replace("$$date_time", or_create_time)
              .replace("$$refund_amount", or_money)
              .replace("$$refund_point", or_point_money)

          println("sqlUpdate:" + sqlUpdate)
          executeSql(sqlUpdate)
        }
      }
    }
  }



  def executePV(upDt: String, shop_id: String, fx_shops: Broadcast[mutable.Map[String, String]]): Unit = {
    val merchant_code = fx_shops.value.getOrElse(shop_id, "unknown")
    val date_time = upDt

    println("insert into fx_day_statistics'" + 1 + "','PV',date('" + date_time + "'));")

    val sqlInsert = "INSERT INTO fx_day_statistics(pv,merchant_code, shop_code, date_time) " +
      "SELECT '" + "0" + "','" + merchant_code + "','" + shop_id + "','" + date_time + "' FROM DUAL " +
      "WHERE NOT EXISTS ( SELECT 1 FROM fx_day_statistics  WHERE merchant_code ='" +
      merchant_code + "' and shop_code='" + shop_id + "' and date_time='" + date_time + "');"

    var sqlUpdate = "UPDATE fx_day_statistics SET pv =pv + " + 1 +
      " WHERE  merchant_code = '" + merchant_code + "' AND shop_code = '" + shop_id + "' AND date_time = '" + date_time + "';"

    println("sqlInsert:" + sqlInsert)
    println("sqlUpdate:" + sqlUpdate)
    executeSql(sqlInsert)
    executeSql(sqlUpdate)
  }

  //  executeUV(conn, fx_shops,device_id,updateTime)
  def executeUV(updateTime: String, shop_id: String, fx_shops: Broadcast[mutable.Map[String, String]]): Unit = {
    val merchant_code = fx_shops.value.getOrElse(shop_id, "unknown")
    val date_time = updateTime
    println("insert into fx_day_statistics'" + 1 + "','UV',date('" + date_time + "'));")

    val sqlInsert = "INSERT INTO fx_day_statistics(uv,merchant_code, shop_code, date_time) " +
      "SELECT '" + "0" + "','" + merchant_code + "','" + shop_id + "','" + date_time + "' FROM DUAL " +
      "WHERE NOT EXISTS ( SELECT 1 FROM fx_day_statistics  WHERE merchant_code ='" +
      merchant_code + "' and shop_code='" + shop_id + "' and date_time='" + date_time + "');"


    var sqlUpdate = "UPDATE fx_day_statistics SET uv =uv + " + 1 +
      " WHERE  merchant_code = '" + merchant_code + "' AND shop_code = '" + shop_id + "' AND date_time = '" + date_time + "';"

    println("sqlInsert:" + sqlInsert)
    println("sqlUpdate:" + sqlUpdate)
    executeSql(sqlInsert)
    executeSql(sqlUpdate)
  }

  //执行SQL
  def executeSql(sql: String) = {
    val conn = JdbcConnectionPool.getConnection.orNull
    try {
      conn.prepareStatement(sql).executeUpdate()
    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      conn.close()
    }
  }

}
