package test

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka.KafkaUtils
/**
  * Created by Administrator on 2017/5/26.
  */
object Kafka2TempTable extends App {
  override def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    // LoggerLevels.setStreamingLogLevels()

    val sparkConf = new SparkConf().setAppName("TestKafka").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    //val ss = SparkSession.builder().config(sparkConf).getOrCreate()
    val ssc = new StreamingContext(sc, Seconds(5))
    val brokers = "jtcrtvdra104:9092"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    val topics = Set("source-mysql-hrtds-fx_shops")
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(x => x._2.split(",", -1))
    //
//    lines.print()

    lines.foreachRDD((rdd: RDD[Array[String]]) => {
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._
      //构造case class: DapLog,提取日志中相应的字段
      val logDataFrame = rdd.map(w => DapLog(w(0).substring(0, 10),w(2),w(6))).toDF()
      //注册为tempTable
      logDataFrame.registerTempTable("daplog")
      //查询该批次的pv,ip数,uv
      val logCountsDataFrame =
        sqlContext.sql("select shop_id,shop_name,shop_desc from daplog")
      //打印查询结果
      logCountsDataFrame.show()
    })



    ssc.start()
    ssc.awaitTermination()
  }


  case class DapLog(shop_id:String, shop_name:String, shop_desc:String)

  object SQLContextSingleton {
    @transient  private var instance: SQLContext = _
    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }

}
