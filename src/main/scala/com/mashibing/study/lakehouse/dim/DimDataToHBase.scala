package com.mashibing.study.lakehouse.dim

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.mashibing.study.lakehouse.utils.{ConfigUtil, MyKafkaUtil, MyStringUtil}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.codehaus.jackson.map.deser.std.StringDeserializer

/**
  *  将Kafka topic  KAFKA-DIM-TOPIC 中的维度数据处理写入Phoenix表
  */
object DimDataToHBase {
  val kafkaBrokers = ConfigUtil.KAFKA_BROKERS
  val consumerKafkaFromEarliest = ConfigUtil.CONSUMER_KAFKA_FROM_EARLIEST
  val kafkaDimTopic = ConfigUtil.KAFKA_DIM_TOPIC
  val phoenixURL = ConfigUtil.PHOENIX_URL
  var ds: DataStream[String] = _

  def main(args: Array[String]): Unit = {
    //1.创建基本环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    //2.设置读取 Kafka 配置
    val props = new Properties()
    props.setProperty("bootstrap.servers",kafkaBrokers)
    props.setProperty("key.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("value.deserializer",classOf[StringDeserializer].getName)
    props.setProperty("group.id","mygroup.id")

    //3.读取Kafka数据
    if(consumerKafkaFromEarliest){
       ds = env.addSource(MyKafkaUtil.getDataFromKafka(kafkaDimTopic,props).setStartFromEarliest())
    }else{
       ds = env.addSource(MyKafkaUtil.getDataFromKafka(kafkaDimTopic,props))
    }

    //ds.print()
    ds.keyBy(value => {
      JSON.parseObject(value).getString("phoenix_tbl_name")
    }).process(new KeyedProcessFunction[String, String, String] {
      lazy private val valueState: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("valueState", classOf[String]))

      var conn : Connection = _
      var pst : PreparedStatement = _

      override def open(parameters: Configuration): Unit = {
        println("连接phoenix")
        conn = DriverManager.getConnection(phoenixURL)
      }

      override def processElement(value: String, context: KeyedProcessFunction[String, String, String]#Context, out: Collector[String]) = {
        val nObject: JSONObject = JSON.parseObject(value)

        //获取建表信息
        val operateType: String = nObject.getString("tp")
        val phxTable: String = nObject.getString("phoenix_tbl_name")
        val pkCol: String = nObject.getString("pk_col")
        val cols: String = nObject.getString("cols")

        if ("insert".equals(operateType) || "update".equals(operateType) || "bootstrap-insert".equals(operateType)) {
          if (valueState.value() == null) {
            createPhoenixTable(phxTable, pkCol,cols)
            valueState.update(phxTable)
          }
          //否则插入数据
          upsertIntoPhoenixTable(nObject, phxTable, pkCol, cols)
        }
        out.collect("向phoenix插入数据执行成功")
      }

      //创建phoenix表
      def createPhoenixTable(phxTable: String, pkCol: String, cols: String) = {
        println("开始创建表...")
        val createSQL = new StringBuilder(s"create table if not exists ${phxTable} (${pkCol} varchar primary key,")
        val colArray: Array[String] = cols.split(",")
        for (col <- colArray) {
          createSQL.append(s"cf.${col} varchar,")
        }
        createSQL.replace(createSQL.length-1, createSQL.length, ") colume_encodes_bytes=0")

        println("phoenix 建表sql：" + createSQL.toString())

        //执行sql
        pst = conn.prepareStatement(createSQL.toString())
        pst.execute()
      }

      def upsertIntoPhoenixTable(nObject: JSONObject, phxTable: String, pkCol: String, cols: String) = {
        val pkValue: String = nObject.getString(pkCol)
        var upsertSQL =new StringBuilder(s"upsert into ${phxTable} values ('${pkValue}'")
        val colArray: Array[String] = cols.split(",")
        for (elem <- colArray) {
          val currentColValue: String = nObject.getString(elem)
          upsertSQL.append(s",'${currentColValue}'")
        }

        upsertSQL.append(s")")
        println("向phoenix插入语句sql = " + upsertSQL.toString())
        pst = conn.prepareStatement(upsertSQL.toString())
        pst.execute()

        conn.commit()
      }

      override def close(): Unit = {
        pst.close()
        conn.close()
      }
    }).print()

    env.execute()


  }
}
