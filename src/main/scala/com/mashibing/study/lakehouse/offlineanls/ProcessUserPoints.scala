package com.mashibing.study.lakehouse.offlineanls

import org.apache.flink.connector.jdbc.JdbcOutputFormat
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
  *  针对 Iceberg -DWS - DWS_BROWSE_INFO 表数据进行用户浏览商品积分分析
  */
object ProcessUserPoints {
  def main(args: Array[String]): Unit = {
    //1.创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tblEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    import org.apache.flink.api.scala._

    //2.创建Catalog
    tblEnv.executeSql(
      """
        | create catalog hadoop_iceberg with (
        |  'type'='iceberg',
        |  'catalog-type'='hadoop',
        |  'warehouse'='hdfs://mycluster/lakehousedata'
        | )
      """.stripMargin)

    //3.使用当前catalog 及使用对应的库 icebergdb
    tblEnv.useCatalog("hadoop_iceberg")
    tblEnv.useDatabase("icebergdb")

    //4.编写SQL 查询数据
    val userPointsTbl:Table = tblEnv.sqlQuery(
      """
        | select
        |   log_time as dt ,user_id,product_name,sum(cast (obtain_points as int)) as total_points
        | from DWS_BROWSE_INFO
        | group by log_time ,user_id ,product_name
      """.stripMargin)

    //5.转换成DataStream ，打印结果
    val userPointDS: DataStream[(Boolean, Row)] = tblEnv.toRetractStream[Row](userPointsTbl)

    //6.将结果写出到MySQL 对应的表中
    /**
      *  在mysql 中创建表： user_points
      *   create table user_points (log_time varchar(255),user_id varchar(255),product_name varchar(255),total_points bigint)
      */
    val jdbcOutputFormat: JdbcOutputFormat = JdbcOutputFormat.buildJdbcOutputFormat()
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://node2:3306/resultdb?user=root&password=123456")
      .setQuery("insert into user_points values (?,?,?,?)")
      .finish()

    userPointDS.map(_._2).writeUsingOutputFormat(jdbcOutputFormat)

    env.execute()

  }

}
