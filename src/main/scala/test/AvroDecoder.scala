package test

/*import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase*/
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import org.apache.avro.Schema
import org.apache.avro.io.{BinaryDecoder, DecoderFactory, DatumReader}
import org.apache.avro.specific.{SpecificRecordBase, SpecificDatumReader}
/**
  * Created by LIUWEI946 on 2017/6/1.
  */
class AvroDecoder[T <: SpecificRecordBase](props: VerifiableProperties = null, schema: Schema)
  extends Decoder[T] {

  private[this] val NoBinaryDecoderReuse = null.asInstanceOf[BinaryDecoder]
  private[this] val NoRecordReuse = null.asInstanceOf[T]
  private[this] val reader: DatumReader[T] = new SpecificDatumReader[T](schema)

  override def fromBytes(bytes: Array[Byte]): T = {
    val decoder = DecoderFactory.get().binaryDecoder(bytes, NoBinaryDecoderReuse)
    reader.read(NoRecordReuse, decoder)
  }

}
/*
class AvroDecoder(props: VerifiableProperties = null) extends Decoder[String] {
  val encoding =
    if (props == null)
      "UTF8"
    else
      props.getString("serializer.encoding", "UTF8")

  def fromBytes(bytes: Array[Byte]): String = {
    new String(bytes, encoding)
  }
}*/
