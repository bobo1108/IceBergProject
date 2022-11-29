package com.mashibing.study.lakehouse.ods

import java.lang
import java.sql.PreparedStatement
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.mashibing.study.lakehouse.utils.{CommonUtil, ConfigUtil, MySQLUtil}
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.connector.jdbc.{JdbcSink, JdbcStatementBuilder}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerRecord

/**
  * 处理 Kafka DB 业务数据，写入ODS层，代码中主要处理逻辑如下：
  * 1.读取Kafka DB 业务数据 “KAFKA-DB-BUSSINESS-DATA”数据
  * 2.将以上读取DB数据写入到Iceberg-ODS层中
  * 3.将以上读取DB数据按照是否是维度数据进行分流写入Kafka 不同Topic中
  *    事实数据写入：KAFKA-ODS-TOPIC
  *    维度数据写入：KAFKA-DIM-TOPIC
  */
object ProduceKafkaDBDataToODS {

  private val mysqlUrl = ConfigUtil.MYSQL_URL
  private val mysqlUser = ConfigUtil.MYSQL_USER
  private val mysqlPassword = ConfigUtil.MYSQL_PASSWORD
  private val kafkaDwdUserLogTopic = ConfigUtil.KAFKA_DWD_USER_LOG_TOPIC
  private val kafkaOdsTopic = ConfigUtil.KAFKA_ODS_TOPIC
  private val kafkaDimTopic = ConfigUtil.KAFKA_DIM_TOPIC
  private val kafkaBrokers = ConfigUtil.KAFKA_BROKERS

  def main(args: Array[String]): Unit = {

    //1.创建基本环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tblEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //2.导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //3.设置checkpoint
    env.enableCheckpointing(5000)
//    env.setStateBackend(new FsStateBackend("hdfs://mycluster/checkpoint/cp1"))

    tblEnv.executeSql(
      """
        | create catalog hadoop_iceberg with(
        | 'type'='iceberg',
        | 'catalog-type'='hadoop',
        | 'warehouse'='hdfs://hadoop102:9000/lakehousedata'
        | )
      """.stripMargin)

    tblEnv.executeSql(
      """
        | create table kafka_db_bussiness_tbl(
        | database string,
        | `table` string,
        | type string,
        | ts string,
        | xid string,
        | `commit` string,
        | data map<string, string>
        | ) with (
        | 'connector' = 'kafka',
        | 'topic' = 'KAFKA-DB-BUSSINESS-DATA',
        | 'properties.bootstrap.servers'='hadoop102:9092,hadoop103:9092,hadoop104:9092',
        | 'scan.startup.mode'='earliest-offset',
        | 'properties.group.id' = 'my-group-id',
        | 'format' = 'json'
        | )
      """.stripMargin)

    //向Iceberg ods 层 ODS_PRODUCT_CATEGORY 表插入数据
//    tblEnv.executeSql(
//      """
//        |insert into hadoop_iceberg.icebergdb.ODS_PRODUCT_CATEGORY
//        |select
//        |   data['id'] as id ,
//        |   data['p_id'] as p_id,
//        |   data['name'] as name,
//        |   data['pic_url'] as pic_url,
//        |   data['gmt_create'] as gmt_create
//        | from kafka_db_bussiness_tbl where `table` = 'pc_product_category'
//      """.stripMargin)

    //向Iceberg ods 层 ODS_PRODUCT_INFO 表插入数据
//    tblEnv.executeSql(
//      """
//        |insert into hadoop_iceberg.icebergdb.ODS_PRODUCT_INFO
//        |select
//        |   data['product_id'] as product_id ,
//        |   data['category_id'] as category_id,
//        |   data['product_name'] as product_name,
//        |   data['gmt_create'] as gmt_create
//        | from kafka_db_bussiness_tbl where `table` = 'pc_product'
//      """.stripMargin)

    //    //写入Iceberg中
//    tblEnv.executeSql(
//      """
//        | insert into hadoop_iceberg.icebergdb.ODS_MEMBER_INFO
//        | select
//        |    data['id'] as id,
//        |    data['user_id'] as user_id,
//        |    data['member_growth_score'] as member_growth_score,
//        |    data['member_level'] as member_level,
//        |    data['balance'] as balance,
//        |    data['gmt_create'] as gmt_create,
//        |    data['gmt_modified'] as  gmt_modified
//        | from kafka_db_bussiness_tbl where `table` = 'mc_member_info'
//      """.stripMargin)
//
//    tblEnv.executeSql(
//      """
//        |insert into hadoop_iceberg.icebergdb.ODS_MEMBER_ADDRESS
//        |select
//        |   data['id'] as id ,
//        |   data['user_id'] as user_id,
//        |   data['province'] as province,
//        |   data['city'] as city,
//        |   data['area'] as area,
//        |   data['address'] as address,
//        |   data['log'] as log,
//        |   data['lat'] as lat,
//        |   data['phone_number'] as phone_number,
//        |   data['consignee_name'] as consignee_name,
//        |   data['gmt_create'] as gmt_create,
//        |   data['gmt_modified'] as  gmt_modified
//        | from kafka_db_bussiness_tbl where `table` = 'mc_member_address'
//      """.stripMargin)

    tblEnv.executeSql(
      """
        |insert into hadoop_iceberg.icebergdb.ODS_USER_LOGIN_TEST
        |select
        |   data['id'] as id ,
        |   data['user_id'] as user_id,
        |   data['ip'] as ip,
        |   data['login_tm'] as login_tm,
        |   data['logout_tm'] as logout_tm
        | from kafka_db_bussiness_tbl where `table` = 'mc_user_login'
      """.stripMargin)


    //读取kafka中数据，将维度数据另存到kafka中
    val kafkaTbl: Table = tblEnv.sqlQuery("select database,`table`,type,ts,xid,`commit`,data from kafka_db_bussiness_tbl")

    //将kafkaTbl Table 转为datastream
    val kafkaDS: DataStream[Row] = tblEnv.toAppendStream[Row](kafkaTbl)

    //设置mapstate，用于广播流
    val mapStateDescriptor = new MapStateDescriptor[String, JSONObject]("mapStateDescriptor", classOf[String], classOf[JSONObject])

    //从mysql中配置信息，并广播
    val bcConfigDs: BroadcastStream[JSONObject] = env.addSource(MySQLUtil.getMySQLData(mysqlUrl, mysqlUser, mysqlPassword)).broadcast(mapStateDescriptor)

    //设置维度数据侧输出流
    val dimDataTag = new OutputTag[String]("dim_data")


    val factMainDs: DataStream[String] = kafkaDS.filter(row => {
      "lakehousedb".equals(row.getField(0).toString)
    }).connect(bcConfigDs)
      .process(new BroadcastProcessFunction[Row, JSONObject, String] {
        override def processElement(row: Row, ctx: BroadcastProcessFunction[Row, JSONObject, String]#ReadOnlyContext, out: Collector[String]) = {
          val returnJsonObj = new JSONObject()

          //获取广播状态
          val robcs: ReadOnlyBroadcastState[String, JSONObject] = ctx.getBroadcastState(mapStateDescriptor)

          //解析事件流
          val nObject: JSONObject = CommonUtil.rowToJson(row)

          //获取当前事件流来自的库与表
          val dbName: String = nObject.getString("database")
          val tableName: String = nObject.getString("table")
          val key = dbName + ":" + tableName

          if (robcs.contains(key)) {
            val js: JSONObject = robcs.get(key)
            //维度数据
            nObject.put("tbl_name", js.getString("tbl_name"))
            nObject.put("tbl_db", js.getString("tbl_db"))
            nObject.put("pk_col", js.getString("pk_col"))
            nObject.put("cols", js.getString("cols"))
            nObject.put("phoenix_tbl_name", js.getString("phoenix_tbl_name"))
            ctx.output(dimDataTag, nObject.toString)
          } else {
            //事实
            if ("mc_user_login".equals(tableName)) {
              returnJsonObj.put("iceberg_ods_tbl_name", "ODS_USER_LOGIN")
              returnJsonObj.put("kafka_dwd_topic", kafkaDwdUserLogTopic)
              returnJsonObj.put("data", nObject.toString)
            }
            out.collect(returnJsonObj.toJSONString)
          }
        }

        override def processBroadcastElement(jsonObject: JSONObject, ctx: BroadcastProcessFunction[Row, JSONObject, String]#Context, collector: Collector[String]) = {
          val tblDB: String = jsonObject.getString("tbl_db")
          val tblName: String = jsonObject.getString("tbl_name")

          //向状态更新数据
          val bcs: BroadcastState[String, JSONObject] = ctx.getBroadcastState(mapStateDescriptor)
          bcs.put(tblDB + ":" + tblName, jsonObject)
          println("广播数据流设置完成...")
        }
      })


    //结果写入kafka
    val props = new Properties()
    props.setProperty("bootstrap.servers",kafkaBrokers)

//    factMainDs.addSink(new FlinkKafkaProducer[String](kafkaOdsTopic, new KafkaSerializationSchema[String] {
//      override def serialize(t: String, aLong: lang.Long) = {
//        new ProducerRecord[Array[Byte], Array[Byte]](kafkaOdsTopic, null, t.getBytes())
//      }
//    },props, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))

    //测流
    factMainDs.getSideOutput(dimDataTag).print()
//    factMainDs.getSideOutput(dimDataTag).addSink(new FlinkKafkaProducer[String](kafkaDimTopic, new KafkaSerializationSchema[String] {
//      override def serialize(t: String, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
//        new ProducerRecord[Array[Byte], Array[Byte]](kafkaDimTopic, null, t.getBytes())
//      }
//    },props, FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))

    env.execute()

  }
}
