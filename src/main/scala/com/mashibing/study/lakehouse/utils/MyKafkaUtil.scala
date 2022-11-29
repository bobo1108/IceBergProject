package com.mashibing.study.lakehouse.utils

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object MyKafkaUtil {
  //消费Kafka 中数据
  def getDataFromKafka(kafkaDimTopic: String, props: Properties) = {
    new FlinkKafkaConsumer[String](kafkaDimTopic,new SimpleStringSchema(),props)
  }

}
