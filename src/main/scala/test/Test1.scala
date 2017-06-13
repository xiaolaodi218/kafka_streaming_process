package test

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by LIUWEI946 on 2017/6/5.
  */
object Test1 {
  def main(args: Array[String]): Unit = {
/*    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")//"dd/MMM/yyyy:HH:mm:ss"
    println(dateFormat.parse("2014-09-04 00:47:59.0"))
    val strFormat1=new SimpleDateFormat("yyyyMMdd")
    println(strFormat1.format(dateFormat.parse("2014-09-04 00:47:59.0")))*/
    //test1()
    //readFile()
    //method1()
    //test()
    //println(getNowDate())

  }

  def getNowDate(): String = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var hehe = dateFormat.format(now)
    hehe
  }

  var toMap: Map[Nothing, Nothing] = _

  def readFile(): Unit = {
    var records = ArrayBuffer[String]()
    var record: String = ""
    val filename="D:/scalaTestFile/fx_shops_streaming.txt"
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
    for(i<- 0 until records.length ){
      println(records(i))
    }
    println(records.length)
  }

  def test1() = {
    var fx_shops: mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()
    var shop_id, merchant_code = ""
    //val record = "Struct{shop_id=125,shop_name=爱家居,shop_desc=爱家居 綠色環保品味生活,由我們開始,shop_contact_addr=深圳灣海德三路海岸城東座7樓709室,shop_flag_path=121994,sl_id=2,shop_status=active,m_id=86033,shop_order=0,shop_create_time=2016-01-18 17:22:33.0,s_main_cate=25,s_intro=Montaco Enterprises Ltd 于2003年于香港创立，我们主要的业务范围为供应摩托车配件 ( 供应给世界知名的摩托车客户,雅马哈 本田等客户 )、仓务库存及业务顾问服务。 由于拥有与中国供货商长期合作的伙伴关系，故我们能有条件地提供具价钱竞争性而又高质量的产品。于2009年, 监于市场机会, 我司设立了家品部门,且与一所日本公司研发了一系列独特的高级陶瓷及硅胶家居用品.   为了配合长远发展, 公司同年创立了两个自己的品牌 Konomi® 和 ZEKOU® 以热诚创造、高质量的要求、及独创性的精神务求能带给顾客一个丰富而优质的生活。现我们正积极地策划扩大本地市场 ( 香港 / 中国 ).公司注册品牌： ZEKOU® 定位主要是以大众化的价钱推广给消费者，务求令每个家庭都能使.  而Konomi®则是走中,高档及礼品路线为主.,c_id=119,shop_type=0,shop_remarks=,shop_logo_path=121995,shop_store_enable=0,shop_send_addr=,shop_send_mobile=,shop_email=,merchantCode=2523300001099,show_purcharse_type=0,s_overseas_purchase=0,s_overseas_purchase_limit=0,merchant_name=,is_announcement=0}"
    //读取文件
    var records = ArrayBuffer[String]()
    var record: String = ""
    val filename="D:/scalaTestFile/fx_shops_streaming.txt"
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
    for(i<- 0 until records.length ){
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

    for ((shop_id, mechancode) <- fx_shops) {
      println(shop_id + ":" + mechancode)
    }
  }

  def method1(): Unit = {
    var s = """Struct{shop_id=125,shop_name=爱家居,shop_desc=爱家居 綠色環保品味生活,由我們開始,shop_contact_addr=深圳灣海德三路海岸城東座7樓709室,shop_flag_path=121994,sl_id=2,shop_status=active,m_id=86033,shop_order=0,shop_create_time=2016-01-18 17:22:33.0,s_main_cate=25,s_intro=Montaco Enterprises Ltd 于2003年于香港创立，我们主要的业务范围为供应摩托车配件 ( 供应给世界知名的摩托车客户,雅马哈 本田等客户 )、仓务库存及业务顾问服务。 由于拥有与中国供货商长期合作的伙伴关系，故我们能有条件地提供具价钱竞争性而又高质量的产品。
              | 于2009年, 监于市场机会, 我司设立了家品部门, 且与一所日本公司研发了一系列独特的高级陶瓷及硅胶家居用品.   为了配合长远发展, 公司同年创立了两个自己的品牌 Konomi® 和 ZEKOU® 以热诚创造、高质量的要求、及独创性的精神务求能带给顾客一个丰富而优质的生活。现我们正积极地策划扩大本地市场 ( 香港 / 中国 ).
              |
              |公司注册品牌： ZEKOU® 定位主要是以大众化的价钱推广给消费者，务求令每个家庭都能使.  而Konomi®则是走中,高档及礼品路线为主.,c_id=119,shop_type=0,shop_remarks=,shop_logo_path=121995,shop_store_enable=0,shop_send_addr=,shop_send_mobile=,shop_email=,merchantCode=2523300001099,show_purcharse_type=0,s_overseas_purchase=0,s_overseas_purchase_limit=0,merchant_name=,is_announcement=0}
              |Struct{shop_id=126,shop_name=宝利嘉,shop_desc=,shop_contact_addr=,shop_flag_path=,sl_id=2,shop_status=active,m_id=86811,shop_order=0,shop_create_time=2016-01-20 15:48:24.0,s_main_cate=3,s_intro=深圳宝利嘉电子商务有限公司，是一家一个B2C+B2B2C为主的进口跨境电商平台，以O2O为理念，线上B2C以移动互联网为基础，平台启动初期就是以抢占手机和微信端用户为目标；线下B2B建立体验店，通过帮助加盟商扩充品类，提供资金垫款，在线下引流的一体化平台。
              |   经验丰富，多元化，有激情的创业团队。
              |   集团公司具有强大的线下物流网络和丰富的供应链金融管理经验。
              |   丰富的供应商品牌资源，100% 正品保证。
              |   总部位于深圳，有完善的物流体系及便利的融资环境 。,c_id=121,shop_type=0,shop_remarks=,shop_store_enable=0,shop_send_addr=,shop_send_mobile=,shop_email=,merchantCode=2659900000119,show_purcharse_type=0,s_overseas_purchase=0,s_overseas_purchase_limit=0,merchant_name=,is_announcement=0}
              |"""
    var results: Array[Array[String]] = s.substring(7, s.length - 1).split(",").map(x => (x.split("=")))

    val resultsMap: mutable.Map[String, String] = scala.collection.mutable.Map[String, String]()
    for (items: Array[String] <- results) {
      if (items.length > 1) {
        //println(items(0) + ":" + items(1))
        resultsMap += (items(0).toString -> items(1).toString)
      }
      else {
        //println(items(0) + ":null")
        resultsMap += (items(0).toString -> "")
      }
    }
    println(resultsMap.get("shop_id"))
    println(resultsMap.getOrElse("shop_id", 0))
    println(resultsMap.get("merchantCode"))
    println(resultsMap.getOrElse("merchantCode", 0))
    //val resultMap:Map[Nothing, Nothing] = results.toMap
    //    val resultMap:Map[Nothing, Nothing] =results.toMap
    //val resultMap=scala.collection.mutable.Map(results.toMap)

    //println(resultMap.getOrElse("m_id",0))
    //println(resultMap.get("o_status")
    /*    for ((k,v) <- resultMap){

        }*/
    /*    val newMap:mutable.Map[String, String] =scala.collection.mutable.Map[String,String]()
        for (entry <- resultMap){
          //if(entry._1 instanceof String){//:Map[String, Object]
            newMap.put(entry._1.toString, entry._2.toString)
        }
        println(newMap.get("o_status"))*/
    /*    val resultMap1:mutable.Map[String, String] =scala.collection.mutable.Map[String,String]()
        for (items: Array[String] <- results;
             item: String <- items) {
          resultMap1++items.toMap
          println(item)
        }*/
    /*    for (i <- 0 until results.length) {
          for (j <- 0 until results(i).length) {
            if(j!=results(i).length-1)
            println(results(i)(j) + ":" + results(i)(j + 1))

          }
        }*/
  }

  def test() = {
    val filename="D:/scalaTestFile/test.txt"
    val file=Source.fromFile(filename)
    file.foreach(print)
    /*    val str = """品牌：秋壳  货号：4Q201    厚薄：常规
                    衣门襟：其他  组合形式：单件 颜色：黑色
                    流行元素：其他 服装版型：直筒 衣长：常规款
                    领型：圆领   风格：通勤   尺码：S,M,L,XS
                    面料材质：聚酯纤维   年份季节：2017年春季"""
        val res = str.replaceAll("(\0|\\s*|\r|\n)", "")//replaceAll("(\0|\\s*|\r|\n)", "")
        print(res)*/
    /*    val s = "Struct{o_id=2016101115522899902,m_id=6255209678659190797,m_name=过回家过分,m_mobile=18589083362,m_telephone=,m_real_name=,m_idcard=83H9SuNVSOyC1eyyRyO5uA,o_goods_total_price=12.0000,o_total_price=12.0000,o_discount=0.0000,o_pay=0.0000,o_coupon=0,c_sn=,o_coupon_menoy=0.0000,o_cost_freight=0.0000,o_payment=5,o_payment_name=微信支付,o_receiver_name=,o_receiver_mobile=,o_receiver_telphone=,o_receiver_province=,o_receiver_city=,o_receiver_district=,o_receiver_address=,cr_path=,o_receiver_zipcode=,o_create_time=2016-10-11 15:52:28.0,o_update_time=2017-04-25 10:12:08.0,o_status=TRADE_CLOSED,o_receiver_email=,lc_name=,lc_id=0,lm_id=0,lm_name=,o_audit=0,o_buyer_comments=,o_seller_comments=,o_partform_comments=,is_invoice=0,po_id=2016101115522861572,flag_type=0,o_after_status=AGREE_NOT_EXIST,shop_id=13,shop_name=测试店铺,o_pay_time=2016-11-15 13:15:40.0,o_notice_type=0,o_completed_time=0002-11-30 00:00:00.0,o_closed_time=2017-04-25 10:12:08.0,o_trade_succ_time=0002-11-30 00:00:00.0,o_logis_time=0002-11-30 00:00:00.0,o_source=ios,store_coupon=0,store_sn=,store_coupon_money=0.0000,o_is_display=1,o_point_consume=2,o_point_money=12.0000,is_notified=0,is_overseas_purchase=0,is_self_delivery=1,out_pay_no=,o_off_money=0.0000}"
        val result = s.substring(7, s.length - 1).split(",").map(x => (x.split("=")))
        for (item <- result) {
          for (field <- item) {
            println(field)
          }
          //println(item)
        }*/
    /*    Map<String,Object> map = new HashMap<String,Object>(); //Object is containing String
    Map<String,String> newMap =new HashMap<String,String>();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      if(entry.getValue() instanceof String){
        newMap.put(entry.getKey(), (String) entry.getValue());
      }
    }*/
    /*    val days = Array("Sunday", "Monday", "Tuesday", "Wednesday","Thursday", "Friday", "Saturday")
        days.zipWithIndex.foreach{case(day,count) => println(s"$count is $day")}*/
    /*    for(i <- 1 to 10) {
      println("i is " + i);
    }
    for(i <- 1 until 10) {
      println("i is " + i);
    }*/
    /*    var myArray : Array[String] = new Array[String](10);
    for(i <- 0 until myArray.length){
      myArray(i) = "value is: " + i;
    }
    for(value : String <- myArray ) {
      println(value);
    }*/
    /*    for(anArray : Array[String] <- myArray;
            aString : String        <- anArray;
            aStringUC = aString.toUpperCase()
            if aStringUC.indexOf("VALUE") != -1;
            if aStringUC.indexOf("5") != -1     ) {
          println(aString);
        }*/
  }
}
