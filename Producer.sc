//> using scala 2.12.18
//> using dep org.apache.kafka:kafka-clients:3.7.0
//> using dep org.apache.avro:avro:1.11.3
//> using dep com.lihaoyi::requests:0.8.2
//> using dep com.lihaoyi::upickle:3.3.0
//> using dep com.typesafe:config:1.4.3
//> using dep org.scalacheck::scalacheck:1.18.0
//> using dep com.github.pureconfig::pureconfig:0.17.6

import java.time.temporal.ChronoUnit

import org.apache.avro.generic.GenericDatumWriter

import java.time.ZoneOffset

import scala.util.Random

import org.apache.kafka.clients.producer._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import requests._
import java.util.Properties
import java.io._
import java.time.LocalDateTime
import scala.collection.JavaConverters._
import org.apache.avro.io.{DatumWriter, EncoderFactory}
import upickle.default.{ReadWriter => RW, macroRW}
import upickle.default._
import org.scalacheck._
import pureconfig._
import pureconfig.generic.auto._

case class SimulationConf(
    numberOfEvents: Int,
    deviceIdPool: Array[Int],
    valueMin: Int,
    valueMax: Int,
    tsDayDeviation: Int
)
case class KafkaConf(topic: String, broker: String, schemaRegistryUrl: String)
case class Measurement(
    val deviceId: Int,
    val value: Double,
    val metricId: Int,
    val ts: Long
)

val configSource = ConfigSource.file("./application.conf")
val kafkaConf = configSource.at("kafka").loadOrThrow[KafkaConf]

val props = buildProperties(kafkaConf)

val producer = new KafkaProducer[String, Array[Byte]](props)
try {
  val schemaResponse = get(
    s"${kafkaConf.schemaRegistryUrl}/subjects/${kafkaConf.topic}-value/versions/latest"
  )

  val parsedPesponse: SchemaRegistryResponse =
    read[SchemaRegistryResponse](schemaResponse.text)
  val schemaId = parsedPesponse.id
  val schemaJson = parsedPesponse.schema

  val schema = new Schema.Parser().parse(schemaJson)

  // Create Avro records and send them to Kafka
  val simulationConf = configSource.at("simulation").loadOrThrow[SimulationConf]
  for (i <- 1 to simulationConf.numberOfEvents) {
    val measurement = generateMeasurement(simulationConf)
    val record = serializeMessage(measurement, schema, schemaId)
    val key = measurement.deviceId.toString
    val kafkaRecord =
      new ProducerRecord[String, Array[Byte]](kafkaConf.topic, key, record)
    producer.send(kafkaRecord).get()
  }
} catch {
  case e: Exception => e.printStackTrace()
} finally {
  producer.close()
}

def generateMeasurement(
    simulationConf: SimulationConf
): Measurement = {

  val now = LocalDateTime.now()

  val measurementGenerator: Gen[Measurement] =
    for {
      id <- Gen.oneOf(simulationConf.deviceIdPool)
      value <- Gen
        .choose[Double](simulationConf.valueMin, simulationConf.valueMax)
      metric <- Gen.choose[Int](1, 5)
      ts <- Gen.choose(
        now
          .minusDays(simulationConf.tsDayDeviation)
          .toEpochSecond(ZoneOffset.UTC) * 1000,
        now.toEpochSecond(ZoneOffset.UTC) * 1000
      )
    } yield Measurement(id, value, metric, ts)

  measurementGenerator.sample.get
}

def serializeMessage(
    measurement: Measurement,
    schema: Schema,
    schemaId: Int
): Array[Byte] = {

  val record = new GenericRecordBuilder(schema)
    .set("deviceId", measurement.deviceId)
    .set("value", measurement.value)
    .set("metricId", measurement.metricId)
    .set("ts", measurement.ts)
    .build()

  // Serialize the Avro record to binary
  val avroDataOutputStream = new ByteArrayOutputStream()
  val avroEncoder =
    EncoderFactory.get().binaryEncoder(avroDataOutputStream, null)

  val magicByte: Byte = 0
  val datumWriter = new GenericDatumWriter[GenericRecord](schema)
  avroDataOutputStream.write(Array(magicByte))
  avroDataOutputStream.write(
    (Array.fill[Byte](4)(0) ++ BigInt(schemaId).toByteArray).takeRight(4)
  )
  datumWriter.write(record, avroEncoder)
  avroEncoder.flush()

  val serializedAvroData = avroDataOutputStream.toByteArray()
  avroDataOutputStream.close()
  serializedAvroData
}

def buildProperties(
    kafkaConf: KafkaConf
): Properties = {
  val properties: Properties = new Properties
  properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConf.broker);
  properties.put(
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer"
  )
  properties.put(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.ByteArraySerializer"
  )
  properties.put("schema.registry.url", kafkaConf.schemaRegistryUrl)
  properties
}

case class SchemaRegistryResponse(id: Int, schema: String)
object SchemaRegistryResponse {
  implicit val rw: RW[SchemaRegistryResponse] = macroRW
}
