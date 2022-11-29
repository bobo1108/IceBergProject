package com.mashibing.study.lakehouse.dwd

import java.lang
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import com.alibaba.fastjson.JSON
import com.mashibing.study.lakehouse.utils.DateUtil
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * 使用通用的一套代码实现处理ODS数据DWD层，针对ODS层数据进行清洗写入到Iceberg-DWD层
  *
  * 1.针对Kafka topic "KAFKA-ODS-TOPIC" 进行清洗写入到Iceberg-DWD层
  * 2.将数据写入到对应的Kafka topic 中
  *
  */

case class DwdInfo (iceberg_ods_tbl_name:String,kafka_dwd_topic:String,browse_product_code:String,browse_product_tpcode:String,user_ip:String,obtain_points:String,
                    user_id1:String,user_id2:String, front_product_url:String,  log_time:String,  browse_product_url:String ,id:String,ip:String, login_tm:String,logout_tm:String)

object ProcessODSDataToDWD {
  def main(args: Array[String]): Unit = {
    //1.创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tblEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //2.导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //3.设置checkpoint
    env.enableCheckpointing(5000)
    //    env.setStateBackend(new FsStateBackend("hdfs://mycluster/checkpoint/cp1"))

    //2.创建Icebrerg的catalog
    tblEnv.executeSql(
      """
        |create catalog hadoop_iceberg with(
        | 'type'='iceberg',
        | 'catalog-type'='hadoop',
        | 'warehouse'='hdfs://hadoop102:9000/lakehousedata'
        |)
      """.stripMargin)

    //3.读取Kafka中数据
    tblEnv.executeSql(
      """
        |create table kafka_ods_tbl(
        | iceberg_ods_tbl_name string,
        | data string,
        | kafka_dwd_topic string
        |) with(
        | 'connector'='kafka',
        | 'topic'='KAFKA-ODS-TOPIC',
        | 'properties.bootstrap.servers'='hadoop102:9092,hadoop103:9092,hadoop104:9092',
        | 'scan.startup.mode'='earliest-offset',
        | 'properties.group.id'='mygroup-id',
        | 'format'='json'
        |)
      """.stripMargin)

    //4.读取Kafka中数据转换成DataStream
    val odsTbl: Table =  tblEnv.sqlQuery(
      """
        |select iceberg_ods_tbl_name,kafka_dwd_topic,data from kafka_ods_tbl
      """.stripMargin)

    val odsDS: DataStream[Row] = tblEnv.toAppendStream[Row](odsTbl)

    val kafkaDataTag = new OutputTag[JSONObject]("kafka_data")
    //对事实表进行清洗
    val dwdDs: DataStream[DwdInfo] = odsDS.filter(row => {
      row.getField(0) != null && row.getField(1) != null && row.getField(2) != null
    }).process(new ProcessFunction[Row, DwdInfo] {
      override def processElement(row: Row, ctx: ProcessFunction[Row, DwdInfo]#Context, out: Collector[DwdInfo]) = {
        val iceberg_ods_tbl_name: String = row.getField(0).toString
        val kafka_dwd_topic: String = row.getField(1).toString
        val data: String = row.getField(2).toString
        val nObject: JSONObject = JSON.parseObject(data)

        //清洗数据
        nObject.put("login_tm", DateUtil.getDateYYYYMMDDHHMMSS(nObject.getString("login_tm")))
        nObject.put("logout_tm", DateUtil.getDateYYYYMMDDHHMMSS(nObject.getString("logout_tm")))
        nObject.put("logTime", DateUtil.getDateYYYYMMDDHHMMSS(nObject.getString("logTime")))

        val browse_product_code: String = nObject.getString("browseProductCode")
        val browse_product_tpcode: String = nObject.getString("browseProductTpCode")
        val user_ip: String = nObject.getString("userIp")
        val obtain_points: String = nObject.getString("obtainPoints")
        val front_product_url: String = nObject.getString("frontProductUrl")
        val log_time: String = nObject.getString("logTime")
        val browse_product_url: String = nObject.getString("browseProductUrl")
        val user_id1: String = nObject.getString("user_id")
        val user_id2: String = nObject.getString("userId")
        val id: String = nObject.getString("id")
        val ip: String = nObject.getString("ip")
        val login_tm: String = nObject.getString("login_tm")
        val logout_tm: String = nObject.getString("logout_tm")

        nObject.put("kafka_dwd_topic", kafka_dwd_topic)
        ctx.output(kafkaDataTag, nObject)

        out.collect(DwdInfo(iceberg_ods_tbl_name, kafka_dwd_topic, browse_product_code, browse_product_tpcode, user_ip, obtain_points,
          user_id1,user_id2, front_product_url, log_time, browse_product_url, id, ip, login_tm, logout_tm))
      }
    })
    val table: Table = tblEnv.fromDataStream(dwdDs)

    //插入 iceberg - dwd 层 会员浏览商品日志信息 :DWD_BROWSELOG
    tblEnv.executeSql(
      s"""
        |insert into hadoop_iceberg.icebergdb.DWD_BROWSELOG
        |select
        | log_time,
        | user_id2,
        | user_ip,
        | front_product_url,
        | browse_product_url,
        | browse_product_tpcode,
        | browse_product_code,
        | obtain_points
        | from ${table} where iceberg_ods_tbl_name = 'ODS_BROWSELOG'
      """.stripMargin)

//    tblEnv.executeSql(
//      s"""
//         |insert into hadoop_iceberg.icebergdb.DWD_USER_LOGIN
//         |select
//         | id,user_id1,ip,login_tm,logout_tm
//         | from ${table} where iceberg_ods_tbl_name = 'ODS_USER_LOGIN'
//      """.stripMargin)


    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop102:9092,hadoop103:9092,hadoop104:9092")
    dwdDs.getSideOutput(kafkaDataTag).addSink(new FlinkKafkaProducer[JSONObject]("KAFKA-DWD-DEFAULT-TOPIC", new KafkaSerializationSchema[JSONObject] {
      override def serialize(t: JSONObject, aLong: lang.Long) = {
        val sinkTopic: String = t.getString("kafka_dwd_topic")
        new ProducerRecord[Array[Byte], Array[Byte]](sinkTopic, null, t.toString.getBytes())
      }
    }, props, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))

    env.execute()
  }

}
