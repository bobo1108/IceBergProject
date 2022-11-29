package com.mashibing.study.lakehouse.dws

import java.lang
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.mashibing.study.lakehouse.utils.Beans.{UserLogin, UserLoginWideInfo}
import com.mashibing.study.lakehouse.utils.{CommonUtil, ConfigUtil, MyRedisUtil, MyStringUtil}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerRecord

/**
  *  解决由于guava包版本与phoenix不匹配，写入不了Iceberg
  */
object ProduceUserlogInToDWS {
  private val hbaseDimMemberInfoTbl = ConfigUtil.HBASE_DIM_MEMBER_INFO
  private val hbaseDimMemberAddressInfoTbl = ConfigUtil.HBASE_DIM_MEMBER_ADDRESS_INFO
  private val kafkaDwsUserLoginWideTopic = ConfigUtil.KAFKA_DWS_USER_LOGIN_WIDE_TOPIC
  private val kafkaBrokers = ConfigUtil.KAFKA_BROKERS

  def main(args: Array[String]): Unit = {

    //1.准备环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tblEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    env.enableCheckpointing(5000)

    import org.apache.flink.streaming.api.scala._

    //2.创建Hadoop Iceberg Catalog
    tblEnv.executeSql(
      """
        |create catalog hadoop_iceberg with(
        | 'type'='iceberg',
        | 'catalog-type'='hadoop',
        | 'warehouse'='hdfs://hadoop102:9000/lakehousedata'
        |)
      """.stripMargin)

    //3.读取Kafka KAFKA-DWD-USER-LOGIN-TOPIC topic 中的数据
    tblEnv.executeSql(
      """
        | create table kafka_dwd_user_login_tbl(
        |   user_id string,
        |   ip string,
        |   gmt_create string,
        |   login_tm string,
        |   logout_tm string,
        |   member_level string,
        |   province string,
        |   city string,
        |   area string,
        |   address string,
        |   member_points string,
        |   balance string,
        |   member_growth_score string,
        |   phone_number string,
        |   consignee_name string,
        |   gmt_modified string
        | ) with (
        |   'connector'='kafka',
        |   'topic'='KAFKA-DWS-USER-LOGIN-WIDE-TOPIC',
        |   'properties.bootstrap.servers'='hadoop102:9092,hadoop103:9092,hadoop104:9092',
        |   'properties.group.id'='mygroup-id',
        |   'scan.startup.mode'='earliest-offset',--latest-offset
        |   'format'='json'
        | )
      """.stripMargin)

    //insert into hadoop_iceberg.icebergdb.ODS_BROWSELOG
    //select
    //  data['logTime'] as log_time,
    //  data['userId'] as user_id,
    //  data['userIp'] as user_ip,
    //  data['frontProductUrl'] as front_product_url,
    //  data['browseProductUrl'] as browse_product_url,
    //  data['browseProductTpCode'] as browse_product_tpcode,
    //  data['browseProductCode'] as browse_product_code,
    //  data['obtainPoints'] as obtain_points
    // from kafka_log_data_tbl where `logtype`='browselog'

    tblEnv.executeSql(
      s"""
         | insert into hadoop_iceberg.icebergdb.DWS_USER_LOGIN
         | select
         |   user_id,
         |   ip,
         |   gmt_create,
         |   login_tm,
         |   logout_tm,
         |   member_level,
         |   province,
         |   city,
         |   area,
         |   address,
         |   member_points,
         |   balance,
         |   member_growth_score
         | from kafka_dwd_user_login_tbl
      """.stripMargin)
//
//    //7、 最终结果侧流写入kafka
//    val props = new Properties()
//    props.setProperty("bootstrap.servers",kafkaBrokers)

    env.execute()
  }

}
