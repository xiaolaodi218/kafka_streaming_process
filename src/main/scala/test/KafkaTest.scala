package test

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by LIUWEI946 on 2017/5/31.
  */
object KafkaTest {
  def main(args: Array[String]): Unit = {
    //LoggerLevels.setStreamingLogLevels()
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("c://ck200")
    //"alog-2016-04-16,alog-2016-04-17,alog-2016-04-18"
    //"Array((alog-2016-04-16, 2), (alog-2016-04-17, 2), (alog-2016-04-18, 2))"
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    data.foreachRDD(r => {
      r.foreach(s => {
        println(s._1)
        println(s._2)
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
