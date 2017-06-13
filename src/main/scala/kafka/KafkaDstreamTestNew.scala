package kafka

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.ClassTag

/**
  * Created by LIUWEI946 on 2017/5/31.
  */

object KafkaDstreamTestNew {
  var fx_shops: mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()
  //shopid->merchantCode
  var fx_orders: mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()

  //o_id->o_status
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TestKafka").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //ssc.checkpoint("c://ck20170605")
    val brokers = "jtcrtvdra104:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "auto.offset.reset" -> "smallest",
      "group.id" -> "lww2017-06-05" //,"enable.auto.commit" -> "false"
    )
    val fxShops = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("source-mysql-hrtds-fx_shops"))
    fxShops.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(partitionOfRecords => {
        //Obtaining Offsets获取偏移量 lw-lw1 0 17 17
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        partitionOfRecords.foreach(oneStream => {
          var shop_id, merchant_code = ""
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
          println(shop_id, merchant_code)
        })
      })
    })
    //fx_shops = fetch_fx_shops()
    //val broadcastVar = ssc.sparkContext.broadcast(fx_shops)
    //TODO 将fx_shops缓存起来
    /*    // 定义
        val yourBroadcast = BroadcastWrapper[mutable.Map[String, String]](ssc, fx_shops)
        yourStream.transform(rdd => {
          //定期更新广播变量
          if (System.currentTimeMillis - 1000 > Conf.updateFreq) {
            yourBroadcast.update(newValue, true)
          }
          // do something else
        })*/

    val topics = Set("source-mysql-hrtds-fx_orders", "source-mysql-hrtds-fx_orders_refunds")
    //"source-mysql-hrtds-fx_orders", "source-mysql-hrtds-fx_orders_refunds"
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    lines.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      var currentTopic = ""
      rdd.foreachPartition(partitionOfRecords => {
        //Obtaining Offsets获取偏移量 lw-lw1 0 17 17
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        currentTopic = s"${o.topic}"

        val conn = ConnectionPool.getConnection.orNull
        try {
          partitionOfRecords.foreach(oneRecode => {
            //println("record: " + oneRecode._2)
            //一条一条记录处理逻辑
            process(conn, currentTopic, oneRecode._2, fx_shops) //broadcastVar.value
          })
        } finally {
          conn.close()
        }
      })
      // some time later, after outputs have completed 提交偏移量
      //lines.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
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

  def process(conn: Connection, currentTopic: String, record: String, fx_shops: mutable.Map[String, String]): Unit = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val strFormat1 = new SimpleDateFormat("yyyyMMdd")
    try {
      var resultsArray: Array[Array[String]] = record.substring(7, record.length - 1).split(",").map(x => (x.split("=")))
      var resultsMap: mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()

      if (currentTopic == "source-mysql-hrtds-fx_orders") {
        //将一条记录从数组转成map
        for (items: Array[String] <- resultsArray) {
          if (items.length > 1) {
            resultsMap += (items(0).toString -> items(1).toString)
          }
          else {
            resultsMap += (items(0).toString -> "")
          }
        }
        val shop_id = resultsMap.getOrElse("shop_id", "")
        val merchant_code = fx_shops.getOrElse(resultsMap.getOrElse("shop_id", ""), "")
        val order_create_time = strFormat1.format(dateFormat.parse(resultsMap.getOrElse("o_create_time", "")))
        val new_order_status = resultsMap.getOrElse("o_status", "")
        var updateField = ""
        val statusSet = mutable.Set("WAIT_BUYER_PAY", "WAIT_SELLER_SEND_GOODS", "ORDER_COMPLETED", "CUSTOMER_SERVICE_PROCESSING") //需要处理的订单状态

        //shop_id merchant_code order_create_time不为空 和
        //当天没有统计过的订单 和
        // 过滤不是当天创建的订单 date_time=strFormat1.format(new Date()) 和
        //订单状态==需要统计指标的状态
        if (shop_id != "" && merchant_code != "" && order_create_time != "" &&
          !fx_orders.contains(resultsMap.getOrElse("o_id", "")) && order_create_time != strFormat1.format(new Date()) &&
          statusSet.contains(new_order_status)) {
          //新订单加入fx_orders：（订单id->订单status）
          if (resultsMap.getOrElse("o_id", "") != "") {
            fx_orders += (resultsMap.getOrElse("o_id", "") -> new_order_status)
          }
          new_order_status match {
            case "WAIT_BUYER_PAY" => updateField = "wait_pay_order"
            case "WAIT_SELLER_SEND_GOODS" => updateField = "wait_delivery_order"
            case "ORDER_COMPLETED" => updateField = "success_order"
            case "CUSTOMER_SERVICE_PROCESSING" => updateField = "after_safe_order"
            case _ => println("this o_status is none of my business,o_status='" + new_order_status + "'")
          }
          //val sqlSuccess="INSERT INTO `kafka`.`fx_orders_result`(`o_id`,`o_create_time`,`o_update_time`,`o_status`,`shop_id`,`shop_name`) VALUES ('"+resultsMap.getOrElse("o_id", 0)+"','"+resultsMap.getOrElse("o_create_time", 0)+"','"+resultsMap.getOrElse("o_update_time", 0)+"','"+resultsMap.getOrElse("o_status", 0)+"','"+resultsMap.getOrElse("shop_id", 0)+"','"+resultsMap.getOrElse("shop_name", 0)+"');"
          //        'WAIT_BUYER_PAY'                     wait_pay_order       待付款订单 o_create_time
          //        'WAIT_SELLER_SEND_GOODS'            wait_delivery_order   待发货订单 o_payed_time
          //        'ORDER_COMPLETED'                    success_order         已完成订单
          //        'CUSTOMER_SERVICE_PROCESSING'       after_safe_order       售后中订单
          //        'ORDER_COMPLETED' o_pay              transaction_amount    成交金额
          //        'ORDER_COMPLETED' o_point_consume    transaction_point     积分
          if (updateField != "") {
            println(currentTopic + ":" + record)
            println("merchant_code:" + merchant_code + " shop_id:" + shop_id + " date_time:" + order_create_time)
            val sqlInsert = "INSERT INTO fx_day_statistics ( merchant_code, shop_code, date_time) " +
              "SELECT '" + merchant_code + "','" + shop_id + "','" + order_create_time + "' FROM DUAL " +
              "WHERE NOT EXISTS ( SELECT * FROM fx_day_statistics  WHERE merchant_code ='" +
              merchant_code + "' and shop_code='" + shop_id + "' and date_time='" + order_create_time + "');"
            println("sqlInsert:" + sqlInsert)
            conn.prepareStatement(sqlInsert).executeUpdate()

            var sqlUpdate = ""
            if (updateField == "success_order") {
              sqlUpdate = "UPDATE fx_day_statistics SET transaction_amount =transaction_amount + " + resultsMap.getOrElse("o_pay", "0") +
                " , transaction_point = transaction_point + " + resultsMap.getOrElse("o_point_consume", "") +
                " , " + updateField + " =" + updateField + "+ 1 " +
                " WHERE  merchant_code = '" + merchant_code + "' AND shop_code = '" + shop_id + "' AND date_time = '" + order_create_time + "'"
            }
            else {
              sqlUpdate = "UPDATE fx_day_statistics SET " + updateField + " =" + updateField + "+ 1 " +
                " WHERE  merchant_code = '" + merchant_code + "' AND shop_code = '" + shop_id + "' AND date_time = '" + order_create_time + "'"
            }
            println("sqlUpdate:" + sqlUpdate)
            if (sqlUpdate != "") {
              conn.prepareStatement(sqlUpdate).executeUpdate()
            }
          }
        }
        //当天已经参与过统计的订单（等价于mysql已经包含对应的merchantCode、shopcode、datetime，无需再insert），
        // 查看订单状态是否变化，没变化不处理，有变化的：一个状态指标加一，一个状态指标减一
        else if (fx_orders.contains(resultsMap.getOrElse("o_id", "")) && order_create_time != strFormat1.format(new Date())) {
          val old_order_status = fx_orders.get(resultsMap.getOrElse("o_id", "")).toString
          var updateFieldBack = ""
          //只处理订单状态发生变化的订单
          if (statusSet.contains(new_order_status) && new_order_status != old_order_status) {
            //            ,sum(case when fo.o_status='WAIT_BUYER_PAY' then 1 else 0 end)                     as wait_pay_order
            //            ,sum(case when fo.o_status='WAIT_SELLER_SEND_GOODS' then 1 else 0 end)             as wait_delivery_order
            //            ,sum(case when fo.o_status='ORDER_COMPLETED' then 1 else 0 end)                    as success_order
            //            ,sum(case when fo.o_status='CUSTOMER_SERVICE_PROCESSING' then 1 else 0 end)        as after_safe_order
            //            ,sum(case when fo.o_status='ORDER_COMPLETED' then fo.o_pay else 0 end )            as transaction_amount
            //            ,sum(case when fo.o_status='ORDER_COMPLETED' then fo.o_point_consume  else 0 end ) as transaction_point
            new_order_status match {
              case "WAIT_BUYER_PAY" => updateField = "wait_pay_order"
              case "WAIT_SELLER_SEND_GOODS" => updateField = "wait_delivery_order"
              case "ORDER_COMPLETED" => updateField = "success_order"
              case "CUSTOMER_SERVICE_PROCESSING" => updateField = "after_safe_order"
              case _ => println("this o_status is none of my business,o_status='" + new_order_status + "'")
            }
            old_order_status match {
              case "WAIT_BUYER_PAY" => updateFieldBack = "wait_pay_order"
              case "WAIT_SELLER_SEND_GOODS" => updateFieldBack = "wait_delivery_order"
              case "ORDER_COMPLETED" => updateFieldBack = "success_order"
              case "CUSTOMER_SERVICE_PROCESSING" => updateFieldBack = "after_safe_order"
              case _ =>
            }
            var sqlUpdate = ""
            //订单状态发生变化，一个状态指标加一，一个状态指标减一
            if (updateField == "success_order") {
              sqlUpdate = "UPDATE fx_day_statistics SET transaction_amount =transaction_amount + " + resultsMap.getOrElse("o_pay", "0") +
                " , transaction_point = transaction_point + " + resultsMap.getOrElse("o_point_consume", "") +
                " , " + updateField + " =" + updateField + "+ 1 , " +
                updateFieldBack + " =" + updateFieldBack + "- 1 " +
                " WHERE  merchant_code = '" + merchant_code + "' AND shop_code = '" + shop_id + "' AND date_time = '" + order_create_time + "'"
            } else {
              sqlUpdate = "UPDATE fx_day_statistics SET " + updateField + " =" + updateField + "+ 1 , " +
                updateFieldBack + " =" + updateFieldBack + "- 1 " +
                " WHERE  merchant_code = '" + merchant_code + "' AND shop_code = '" + shop_id + "' AND date_time = '" + order_create_time + "'"
              println("sqlUpdate:" + sqlUpdate)
            }
            conn.prepareStatement(sqlUpdate).executeUpdate()
          }
        }

      }

      else if (currentTopic == "source-mysql-hrtds-fx_orders_refunds") {
        println(currentTopic + ":" + record)
        for (items: Array[String] <- resultsArray) {
          if (items.length > 1) {
            resultsMap += (items(0).toString -> items(1).toString)
          }
          else {
            resultsMap += (items(0).toString -> "")
          }
        }
        val shop_id = resultsMap.getOrElse("shop_id", "")
        val merchant_code = fx_shops.getOrElse(resultsMap.getOrElse("shop_id", ""), "")
        val order_create_time = strFormat1.format(dateFormat.parse(resultsMap.getOrElse("or_create_time", "")))
        val new_order_status = resultsMap.getOrElse("or_processing_status", "")
        //        var updateField = ""
        if (shop_id != "" && merchant_code != "" && order_create_time != "" &&
          !fx_orders.contains(resultsMap.getOrElse("o_id", "")) && order_create_time != strFormat1.format(new Date())) {
          println(currentTopic + ":" + record)
          println("merchant_code:" + merchant_code + " shop_id:" + shop_id + " date_time:" + order_create_time)
          //新订单加入fx_orders：（订单id->订单status）
          if (resultsMap.getOrElse("o_id", "") != "") {
            fx_orders += (resultsMap.getOrElse("o_id", "") -> new_order_status)
          }
          println("merchant_code:" + merchant_code + " shop_id:" + shop_id + " date_time:" + order_create_time)
          val sqlInsert = "INSERT INTO fx_day_statistics ( merchant_code, shop_code, date_time) " +
            "SELECT '" + merchant_code + "','" + shop_id + "','" + order_create_time + "' FROM DUAL " +
            "WHERE NOT EXISTS ( SELECT * FROM fx_day_statistics  WHERE merchant_code ='" +
            merchant_code + "' and shop_code='" + shop_id + "' and date_time='" + order_create_time + "');"
          println("sqlInsert:" + sqlInsert)
          conn.prepareStatement(sqlInsert).execute()

          var sqlUpdate = ""
          sqlUpdate = "UPDATE fx_day_statistics SET refund_amount =refund_amount + " + resultsMap.getOrElse("or_money", "0") +
            " , refund_point = refund_point + " + resultsMap.getOrElse("or_point_money", "") +
            " WHERE  merchant_code = '" + merchant_code + "' AND shop_code = '" + shop_id + "' AND date_time = '" + order_create_time + "'"

          println("sqlUpdate:" + sqlUpdate)
          conn.prepareStatement(sqlUpdate).executeUpdate()

        }
        else if (shop_id != "" && merchant_code != "" && order_create_time != "" &&
          fx_orders.contains(resultsMap.getOrElse("o_id", "")) && order_create_time != strFormat1.format(new Date())
          && fx_orders.get(resultsMap.getOrElse("o_id", "")) != "REFUND_SUCCESS") {
          println(currentTopic + ":" + record)
          println("merchant_code:" + merchant_code + " shop_id:" + shop_id + " date_time:" + order_create_time)

          var sqlUpdate = ""
          sqlUpdate = "UPDATE fx_day_statistics SET refund_amount =refund_amount + " + resultsMap.getOrElse("or_money", "0") +
            " , refund_point = refund_point + " + resultsMap.getOrElse("or_point_money", "") +
            " WHERE  merchant_code = '" + merchant_code + "' AND shop_code = '" + shop_id + "' AND date_time = '" + order_create_time + "'"

          println("sqlUpdate:" + sqlUpdate)
          conn.prepareStatement(sqlUpdate).executeUpdate()
        }
      }


    } catch {
      case exception: Exception =>
      //logger.warn("Error in execution of query"+exception.printStackTrace())
    }
  }

  //从本地文件读取fx_shops
  def fetch_fx_shops(): mutable.Map[String, String] = {
    var fx_shops: mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()
    var shop_id, merchant_code = ""
    //读取文件
    var records = ArrayBuffer[String]()
    var record: String = ""
    val filename = "D:/scalaTestFile/fx_shops_streaming.txt"
    for (line <- Source.fromFile(filename).getLines) {
      if (!line.endsWith("}")) {
        record = record.concat(line)
      }
      else {
        record = record.concat(line)
        records += record
        record = ""
      }
    }
    for (i <- 0 until records.length) {
      var oneRecode: Array[Array[String]] = records(i).substring(7, records(i).length - 1).replaceAll("\\s*|\t|\r|\n", "").split(",").map(x => (x.split("="))) //.replaceAll("(\0|\\s*|\r|\n)", "")
      for (items: Array[String] <- oneRecode) {
        if (items.length > 1 && items(0).toString == "shop_id") {
          shop_id = items(1).toString
        }
        if (items.length > 1 && items(0).toString == "merchantCode") {
          merchant_code = items(1).toString
        }
      }
      fx_shops += (shop_id -> merchant_code)
    }
    fx_shops
    /* for ((shop_id, mechancode) <- fx_shops) {
       println(shop_id + ":" + mechancode)
     }*/
  }

  case class BroadcastWrapper[T: ClassTag](@transient private val ssc: StreamingContext, @transient private val _v: T) {

    @transient private var v = ssc.sparkContext.broadcast(_v)

    def update(newValue: T, blocking: Boolean = false): Unit = {
      // 删除RDD是否需要锁定
      v.unpersist(blocking)
      v = ssc.sparkContext.broadcast(newValue)
    }

    def value: T = v.value

    private def writeObject(out: ObjectOutputStream): Unit = {
      out.writeObject(v)
    }

    private def readObject(in: ObjectInputStream): Unit = {
      v = in.readObject().asInstanceOf[Broadcast[T]]
    }
  }

  object SQLContextSingleton {
    @transient private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }

 // case class fx_shops_c(shop_id: String, merchantCode: String)

}
