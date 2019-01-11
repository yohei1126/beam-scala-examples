package org.apache.beam.examples

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.options.{Default, Description, PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.io.kafka.{KafkaIO, KafkaRecord}
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}
import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.collect.ImmutableList
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

object SimpleKafkaToBQ {

  def main(args: Array[String]): Unit = {

    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[SimpleKafkaToBQOptions])

    val schema = new TableSchema().setFields(
      ImmutableList.of(
        new TableFieldSchema().setName("text1").setType("STRING"),
        new TableFieldSchema().setName("text2").setType("STRING"),
        new TableFieldSchema().setName("text3").setType("STRING")))

    val p = Pipeline.create(options)
    p.apply("ReadFromKafka", KafkaIO.read()
      .withBootstrapServers(options.getBootstreapServers)
      .withTopics(util.Arrays.asList(options.getKafkaTopic))
      .withKeyDeserializer(classOf[LongDeserializer])
      .withValueDeserializer(classOf[StringDeserializer]))
    // convert PCollection[KafkaRecord] (KafkaIO response) to Message object
    .apply(ParDo.of(new ConvertTextToMessage))
    .apply("WriteToBQ", BigQueryIO.write()
        .to(options.getDestTable)
        .withSchema(schema)
        .withFormatFunction(new ConvertMessageToTable())
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND))
    p.run
  }
}

// ======================================= Options =============================================

trait SimpleKafkaToBQOptions extends PipelineOptions {
  import org.apache.beam.sdk.options.Validation

  @Description("bootstrap server")
  @Validation.Required
  def getBootstreapServers: String
  def setBootstreapServers(servers: String)

  @Description("Kafka topic to subscribe")
  @Default.String("topic1")
  def getKafkaTopic: String
  def setKafkaTopic(topic: String)

  @Description("BigQuery table to write to, specified as <project_id>:<dataset_id>.<table_id>")
  @Validation.Required
  def getDestTable: String
  def setDestTable(destTable: String): Unit
}

// ======================================== UDFs ===============================================

case class Message(text: String, number: Int, timestamp: String)

class ConvertMessageToTable extends SerializableFunction[Message, TableRow] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[ConvertMessageToTable])
  def apply(record: Message): TableRow = {
    logger.info(s"message = $record")
    new TableRow()
      .set("text", record.text)
      .set("number", record.number)
      .set("timestamp", record.timestamp)
  }
}

class ConvertTextToMessage extends DoFn[KafkaRecord[java.lang.Long, String], Message]() {
  private val logger: Logger = LoggerFactory.getLogger(classOf[ConvertTextToMessage])
  @ProcessElement
  def processElement(c: ProcessContext): Unit = {
    try {
      val mapper: ObjectMapper = new ObjectMapper()
        .registerModule(DefaultScalaModule)
      val messageText = c.element.getKV.getValue
      logger.info(s"raw message =  $messageText")
      val record = mapper.readValue(messageText, classOf[Message])
      logger.info(s"record = $record")
      c.output(record)
    } catch {
      case e: Exception =>
        val message = e.getLocalizedMessage + e.getStackTrace
        logger.error(message)
    }
  }
}