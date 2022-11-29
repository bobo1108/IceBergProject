package com.mashibing.study.lakehouse.ods

import java.lang
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.mashibing.study.lakehouse.utils.ConfigUtil
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.kafka.clients.producer.ProducerRecord

/**
  *  从Kafka - KAFKA-USER-LOG-DATA topic读取数据
  *  处理用户日志数据到Kafka ODS - KAFKA-ODS-TOPIC
  */
object ProduceKafkaLogDataToODS {

  val kafkaBrokers: String = ConfigUtil.KAFKA_BROKERS
  val kafkaDwdBrowseLogTopic: String = ConfigUtil.KAFKA_DWD_BROWSE_LOG_TOPIC
  val kafkaOdsTopic: String = ConfigUtil.KAFKA_ODS_TOPIC

  def main(args: Array[String]): Unit = {
    //1.准备环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tblEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    env.enableCheckpointing(5000)

    //2.导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //3.设置Iceberg Catalog - hadoop Catalog
    tblEnv.executeSql(
      """
        |create catalog hadoop_iceberg with(
        | 'type'='iceberg',
        | 'catalog-type'='hadoop',
        | 'warehouse'='hdfs://hadoop102:9000/lakehousedata'
        |)
      """.stripMargin)

    //4.读取Kafka KAFKA-USER-LOG-DATA 中的数据
    tblEnv.executeSql(
      """
        | create table kafka_log_data_tbl(
        |   logtype string,
        |   data map<string,string>
        | ) with (
        |  'connector'='kafka',
        |  'properties.bootstrap.servers'='hadoop102:9092,hadoop103:9092,hadoop104:9092',
        |  'topic'='KAFKA-USER-LOG-DATA',
        |  'scan.startup.mode'='earliest-offset',-- earliest-offset
        |  'properties.group.id'='my-group-id',
        |  'format'='json'
        | )
      """.stripMargin)

    //5.将对应的数据写入到Iceberg - ODS 层表中 ODS_BROWSELOG
    tblEnv.executeSql(
      """
        |insert into hadoop_iceberg.icebergdb.ODS_BROWSELOG
        |select
        |  data['logTime'] as log_time,
        |  data['userId'] as user_id,
        |  data['userIp'] as user_ip,
        |  data['frontProductUrl'] as front_product_url,
        |  data['browseProductUrl'] as browse_product_url,
        |  data['browseProductTpCode'] as browse_product_tpcode,
        |  data['browseProductCode'] as browse_product_code,
        |  data['obtainPoints'] as obtain_points
        | from kafka_log_data_tbl where `logtype`='browselog'
      """.stripMargin)

    //6.对数据进行组织处理，写入到Kafka ODS - KAFKA-ODS-TOPIC 。
    val kafkaLogTbl: Table = tblEnv.sqlQuery("select logtype,data from kafka_log_data_tbl")
    //7.将Kafka 中的数据转换成DataStream处理
    val userLogDS: DataStream[Row] = tblEnv.toAppendStream[Row](kafkaLogTbl)
    //browselog,{browseProductCode=demHQ6TADV, browseProductTpCode=14000,
    // userIp=30.121.149.246, obtainPoints=42, userId=uid813665, frontProductUrl = ,
    // logTime=1655274222161, browseProductUrl=https://2by/a9UWkE/npF76q}

    val odsSinkDataStream: DataStream[String] = userLogDS.map(row => {
      //7.1 准备写出的json
      val returnJsonObject = new JSONObject()
      val logType: String = row.getField(0).toString
      val data: String = row.getField(1).toString
      val arr: Array[String] = data.stripPrefix("{").stripSuffix("}").split(",")
      val nObject = new JSONObject()
      for (elem <- arr) {
        if (elem.contains("=") && elem.split("=").length == 2) {
          val splits: Array[String] = elem.split("=")
          nObject.put(splits(0).trim, splits(1).trim)
        } else {
          nObject.put(elem.trim.stripSuffix("=").trim, "")
        }
      }

      //7.2 判断日志类型，根据日志类型来设置每条数据未来要去往的DWD层的 topic信息
      if ("browselog".equals(logType)) {
        returnJsonObject.put("iceberg_ods_tbl_name", "ODS_BROWSELOG")
        returnJsonObject.put("kafka_dwd_topic", kafkaDwdBrowseLogTopic)
        returnJsonObject.put("data", nObject.toString)
      } else {
        //7.3 其他日志类型
      }

      returnJsonObject.toString
    })

    //8.将以上数据写入到Kafka ODS  KAFKA-ODS-TOPIC 中
    val props = new Properties()
    props.setProperty("bootstrap.servers",kafkaBrokers)

    odsSinkDataStream.addSink(new FlinkKafkaProducer[String](kafkaOdsTopic,new KafkaSerializationSchema[String] {
      override def serialize(element: String, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        new ProducerRecord[Array[Byte], Array[Byte]](kafkaOdsTopic,null,element.getBytes())
      }
    },props,FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))

    env.execute()
  }

}
