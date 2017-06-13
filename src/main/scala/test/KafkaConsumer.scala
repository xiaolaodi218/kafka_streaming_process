package test


import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.io.DatumReader
import org.apache.avro.io.Decoder
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import kafka.consumer.{Consumer, ConsumerConfig, ConsumerTimeoutException, Whitelist}
import kafka.serializer.DefaultDecoder

import scala.io.Source

object KafkaConsumer{
  def main(args: Array[String]): Unit = {
    val consumer=new KafkaConsumer()
    consumer.read()
  }

}
class KafkaConsumer() {
  private val props = new Properties()

  val groupId = "lw"
  val topic = "lei-testlei1"

  props.put("group.id", groupId)
  props.put("zookeeper.connect", "10.0.53.126:2181")
  props.put("auto.offset.reset", "smallest")
  props.put("consumer.timeout.ms", "120000")
  props.put("auto.commit.interval.ms", "10000")

  private val consumerConfig = new ConsumerConfig(props)
  private val consumerConnector = Consumer.create(consumerConfig)
  private val filterSpec = new Whitelist(topic)
  private val streams = consumerConnector.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder())(0)

  lazy val iterator = streams.iterator()

  //read avro schema file
  val schemaString = Source.fromURL(getClass.getResource("/schema.avsc")).mkString
  // Initialize schema
  val schema: Schema = new Schema.Parser().parse(schemaString)


  def read() =
    try {
      if (hasNext) {
        println("Getting message from queue.............")
        val message: Array[Byte] = iterator.next().message()
        val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
        val decoder: Decoder = DecoderFactory.get().binaryDecoder(message, null)
        val userData: GenericRecord = reader.read(null, decoder)

        println(userData.get("id").toString.toInt+"-------"+userData.get("name").toString)
      } else {
        println("have no data------------")
        None
      }
    } catch {
      case ex: Exception => ex.printStackTrace()
        None
    }

  private def hasNext: Boolean =
    try
      iterator.hasNext()
    catch {
      case timeOutEx: ConsumerTimeoutException =>
        false
      case ex: Exception => ex.printStackTrace()
        println("Got error when reading message ")
        false
    }

 /* private def getUser(message: Array[Byte]) = {

    // Deserialize and create generic record
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(message, null)
    val userData: GenericRecord = reader.read(null, decoder)

    // Make user object
    val user = User(userData.get("id").toString.toInt, userData.get("name").toString, try {
      Some(userData.get("email").toString)
    } catch {
      case _ => None
    })
    Some(user)
  }*/

}
