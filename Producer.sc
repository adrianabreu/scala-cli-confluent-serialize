//> using scala 2.12.18
//> using dep org.apache.kafka:kafka-clients:3.5.1
//> using dep org.apache.avro:avro:1.11.3
//> using dep com.lihaoyi::requests:0.8.0

//> using repository https://packages.confluent.io/maven/
//> using dep io.confluent:kafka-avro-serializer:7.4.0
//> using dep com.typesafe:config:1.2.1
import java.time.temporal.ChronoUnit

import org.apache.avro.generic.GenericDatumWriter

import java.time.ZoneOffset

import scala.util.Random

import org.apache.kafka.clients.producer._
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import requests._
import java.util.Properties
import com.typesafe.config._
import java.io._
import java.time.LocalDateTime
import scala.collection.JavaConverters._
import org.apache.avro.io.{DatumWriter, EncoderFactory}
import org.apache.kafka.common.serialization.ByteArraySerializer

case class Measurement(deviceId: Int, value: Double, ts: Long)

val conf = ConfigFactory.parseFile(new File("./defaults.conf"))

val kafkaConf = conf.getConfig("kafka")
val topicName = kafkaConf.getString("topic")
val schemaRegistryUrl = kafkaConf.getString("schemaRegistryUrl")
val props = buildProperties(kafkaConf.getString("broker"), schemaRegistryUrl)

val producer = new KafkaProducer[String, GenericRecord](props)
try {
  val schemaResponse = get(
    s"$schemaRegistryUrl/subjects/$topicName-value/versions/latest/schema"
  )
  val schemaJson = schemaResponse.text

  val schema = new Schema.Parser().parse(schemaJson)

  // Create Avro records and send them to Kafka
  val simulationConf = conf.getConfig("simulation")
  for (i <- 1 to simulationConf.getInt("numberOfEvents")) {
    val measurement = generateMeasurement(
      simulationConf.getStringList("idPool").asScala.toArray.map(_.toInt),
      simulationConf.getInt("valueMin"),
      simulationConf.getInt("valueMax"),
      simulationConf.getInt("timestampMaxDeviationInDays")
    )
    val record = serializeMessage(measurement, schema)
    val key = measurement.deviceId.toString
    val kafkaRecord =
      new ProducerRecord[String, GenericRecord](topicName, key, record)
    producer.send(kafkaRecord).get()
  }
} catch {
  case e: Exception => e.printStackTrace()
} finally {
  producer.close()
}

def generateMeasurement(
    poolIds: Array[Int],
    valueMin: Int,
    valueMax: Int,
    timestampDeviationInDays: Int
): Measurement = {
  Measurement(
    poolIds(Random.nextInt(poolIds.size)),
    Random.nextDouble * valueMax,
    LocalDateTime
      .now()
      .plusDays(Random.nextInt(timestampDeviationInDays))
      .toEpochSecond(ZoneOffset.UTC)
  )
}

def serializeMessage(
    measurement: Measurement,
    schema: Schema
): GenericRecord = {
  val record = new GenericRecordBuilder(schema)
    .set("deviceId", measurement.deviceId)
    .set("value", measurement.value)
    .set("ts", measurement.ts)
    .build()

  record
}

def buildProperties(
    bootstrapUrl: String,
    schemaRegistryUrl: String
): Properties = {
  val properties: Properties = new Properties
  properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapUrl);
  properties.put(
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer"
  )
  properties.put(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "io.confluent.kafka.serializers.KafkaAvroSerializer"
  )
  properties.put("schema.registry.url", schemaRegistryUrl)
  properties
}
