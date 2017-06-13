package com.crc.bigdata

import scala.xml.{NodeSeq, XML}

/**
  * Created by zhanglei852 on 2017/2/7.
  */
class CommonConf(confFileName: String = "config.xml") {

  val conf_file_name = confFileName

  val env = {
    System.getenv("ENV") match {
      case "dev" => "dev_"
      case "test" => "test_"
      case "uat" => "uat_"
      case _ => ""
    }
  }
  val dom = {
    val is = Thread.currentThread().getContextClassLoader.getResourceAsStream(conf_file_name)
    if (is == null)
      throw new Exception("The configuration file [" + confFileName + "] was not found.")
    else {
      val d = XML.load(is)
      is.close()
      d
    }
  }

  def getSparkProperty(sparkName: String, propertyName: String): String = {
    val s = ((dom\"spark_conf").filter(n => ((n\"name").text == env + sparkName))\propertyName).text
    return {
      if (s == null)
        null
      else if("".equals(s.trim))
        null
      else
        s.trim
    }
  }

  def getDBConnectionProperty(dbConnectionName: String, propertyName: String): String = {
    val s = ((dom\"db_connection").filter(n=>((n\"name").text == env + dbConnectionName))\propertyName).text
    return {
      if (s == null)
        null
      else if("".equals(s.trim))
        null
      else
        s.trim
    }
  }

  def getKafkaProperty(dbConnectionName: String, propertyName: String): String = {
    val s = ((dom\"kafka_conf").filter(n=>((n\"name").text == env + dbConnectionName))\propertyName).text
    return {
      if (s == null)
        null
      else if("".equals(s.trim))
        null
      else
        s.trim
    }
  }

  def getEmailProperty(propertyName: String): String = {
    val s = (dom\"email_conf"\propertyName).text
    return {
      if (s == null)
        null
      else if("".equals(s.trim))
        null
      else
        s.trim
    }
  }



  def getEtlProcessPropertyArray(etlProcess: String, propertyName: String): Array[String] = {
    var rs = new Array[String](0)
    val ss = ((dom \ "etlprocesses").filter(n => ((n \ "name").text == etlProcess)) \ propertyName).foreach(n => {
      if (!"".equals(n.text.trim)) rs :+ n.text.trim
    })
    return rs
  }

  def getEtlProcessPropertyNodeList(etlProcess: String, propertyName: String): NodeSeq = {
    var rs = (dom\"etlprocess").filter(n => ((n\"name").text == etlProcess)) \ propertyName
    return rs
  }
}
