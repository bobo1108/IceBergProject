package com.mashibing.study.lakehouse.dws

import com.mashibing.study.lakehouse.utils.ConfigUtil
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
  *  解决由于guava包版本与phoenix不匹配，写入不了Iceberg
  */
object ProductBrowseLogToDWS {
  def main(args: Array[String]): Unit = {

    //1.准备环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tblEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    env.enableCheckpointing(5000)

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
        | create table kafka_dws_browse_log_wide_tbl(
        |   log_time string,
        |   user_id string,
        |   user_ip string,
        |   product_name string,
        |   front_product_url string,
        |   browse_product_url string,
        |   first_category_name string,
        |   second_category_name string,
        |   obtain_points string
        | ) with (
        |   'connector'='kafka',
        |   'topic'='KAFKA-DWS-BROWSE-LOG-WIDE-TOPIC',
        |   'properties.bootstrap.servers'='hadoop102:9092,hadoop103:9092,hadoop104:9092',
        |   'properties.group.id'='mygroup-id',
        |   'scan.startup.mode'='earliest-offset',--latest-offset
        |   'format'='json'
        | )
      """.stripMargin)

    tblEnv.executeSql(
      s"""
         | insert into hadoop_iceberg.icebergdb.DWS_BROWSE_INFO
         | select
         |   log_time as logTime,
         |   user_id as userId,
         |   user_ip as userIp,
         |   product_name as productName,
         |   front_product_url as frontProductUrl,
         |   browse_product_url as browseProductUrl,
         |   first_category_name as firstCategoryName,
         |   second_category_name as secondCategoryName,
         |   obtain_points as obtainPoints
         | from kafka_dws_browse_log_wide_tbl
      """.stripMargin)

    env.execute()
  }

}
