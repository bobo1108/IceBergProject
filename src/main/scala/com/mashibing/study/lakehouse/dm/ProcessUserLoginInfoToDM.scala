package com.mashibing.study.lakehouse.dm

import java.sql.PreparedStatement

import com.mashibing.study.lakehouse.utils.Beans.UserLoginWideInfo
import com.mashibing.study.lakehouse.utils.{DateUtil, MyClickhouseUtil}
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
  *  读取用户主题宽表获取相应的列写入到 DM - Clickhouse中
  */
object ProcessUserLoginInfoToDM {
  def main(args: Array[String]): Unit = {
    //1.准备环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tblEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    import org.apache.flink.streaming.api.scala._

    //2.配置kafka connector ，映射 KAFKA-DWS-USER-LOGIN-WIDE-TOPIC 中的数据
    tblEnv.executeSql(
      """
        | create table kafka_dws_user_login_wide_tbl(
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
        |   member_growth_score string
        | ) with (
        |  'connector'='kafka',
        |  'topic' = 'KAFKA-DWS-USER-LOGIN-WIDE-TOPIC',
        |  'properties.bootstrap.servers'='hadoop102:9092,hadoop103:9092,hadoop104:9092',
        |  'scan.startup.mode'='earliest-offset',--latest-offset
        |  'properties.group.id'='my-group-id',
        |  'format'='json'
        | )
      """.stripMargin)

    //3.读取 Kafka 用户主题数据，获取对应的列
    val dwsTbl : Table = tblEnv.sqlQuery(
      """
        | select province,city,user_id,login_tm,gmt_create from kafka_dws_user_login_wide_tbl
      """.stripMargin)

    //4 将Table 转换成DataStream，方便后期写入到Clickhouse - DM层中
    val dmDS: DataStream[UserLoginWideInfo] = tblEnv.toAppendStream[Row](dwsTbl)
      .filter(row => {
        row.getField(0) != null
      })
      .map(row => {
        val province: String = row.getField(0).toString
        val city: String = row.getField(1).toString
        val user_id: String = row.getField(2).toString
        val login_tm: String = row.getField(3).toString
        val gmt_create: String = row.getField(4).toString
        UserLoginWideInfo(user_id, null, login_tm, null, null, null, null, null, DateUtil.getDateYYYYMMDDHHMMSS(gmt_create), province, city, null, null, null, null)
      })

    //6 调用封装方法得到Clickhouse Sink
    /**
      * create table dm_user_login_info(
      *  dt String,
      *  province String,
      *  city String,
      *  user_id String,
      *  login_tm String,
      *  gmt_create String)engine=MergeTree() order by dt;
      *
      */
    val insertSQL = "insert into dm_user_login_info (dt,province,city,user_id,login_tm,gmt_create) values (?,?,?,?,?,?)"
    val sinkFun: SinkFunction[UserLoginWideInfo] = MyClickhouseUtil.clickhouseSink[UserLoginWideInfo](insertSQL,
      new JdbcStatementBuilder[UserLoginWideInfo] {
                  override def accept(pst: PreparedStatement, userLoginWideInfo: UserLoginWideInfo): Unit = {
                    val province: String = userLoginWideInfo.province
                    val city: String = userLoginWideInfo.city
                    val user_id: String = userLoginWideInfo.user_id
                    val login_tm: String = userLoginWideInfo.login_tm
                    val gmt_create: String = userLoginWideInfo.gmt_create
                    pst.setString(1,DateUtil.getCurrentDateYYYYMMDD())
                    pst.setString(2,province)
                    pst.setString(3,city)
                    pst.setString(4,user_id)
                    pst.setString(5,login_tm)
                    pst.setString(6,gmt_create)
                  }
                })
    dmDS.addSink(sinkFun)
    env.execute()

  }

}
