package com.mashibing.study.lakehouse.dm

import java.sql.PreparedStatement

import com.mashibing.study.lakehouse.utils.Beans.{BrowseLogWideInfo, ProductVisitInfo}
import com.mashibing.study.lakehouse.utils.{DateUtil, MyClickhouseUtil}
import org.apache.flink.connector.jdbc.JdbcStatementBuilder
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.util.Collector
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
  *  处理用户浏览商品数据 DWS 数据 - KAFKA-DWS-BROWSE-LOG-WIDE-TOPIC 写入到DM层 - Clickhouse(?)
  *
  */
object ProcessBrowseLogInfoToDM {
  def main(args: Array[String]): Unit = {
    //1.准备环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tblEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    env.enableCheckpointing(5000)

    import org.apache.flink.streaming.api.scala._

    //2.读取 Kafka DWS  - KAFKA-DWS-BROWSE-LOG-WIDE-TOPIC 中的数据
    tblEnv.executeSql(
      """
        | create table kafka_dws_browse_log_wide_tbl(
        |  user_id string,
        |  product_name string,
        |  first_category_name string,
        |  second_category_name string,
        |  obtain_points string
        | ) with(
        | 'connector'='kafka',
        | 'topic'='KAFKA-DWS-BROWSE-LOG-WIDE-TOPIC',
        | 'properties.bootstrap.servers'='hadoop102:9092,hadoop103:9092,hadoop104:9092',
        | 'scan.startup.mode'='earliest-offset',-- earliest-offset,latest-offset
        | 'properties.group.id'='my-group-id',
        | 'format'='json'
        | )
      """.stripMargin)
    //3.读取kafka 表 kafka_dws_browse_log_wide_tbl 数据
    val dwsTbl:Table = tblEnv.sqlQuery(
      """
        | select user_id,product_name,first_category_name,second_category_name,obtain_points from kafka_dws_browse_log_wide_tbl
      """.stripMargin)

    //4.将读取到的数据转换成对象类型的DataStream
    val browseLogDS: DataStream[BrowseLogWideInfo] = tblEnv.toAppendStream[Row](dwsTbl).map(row => {
      val user_id: String = row.getField(0).toString
      val product_name: String = row.getField(1).toString
      val first_category_name: String = row.getField(2).toString
      val second_category_name: String = row.getField(3).toString
      val obtain_points: String = row.getField(4).toString
      BrowseLogWideInfo(null, user_id, null, product_name, null, null, first_category_name, second_category_name, obtain_points)
    })

    browseLogDS.print()

    // BrowseLogWideInfo(null,uid289100,null,汽车玻璃,null,null,汽车用品,维修保养,48)
    //5.设置keyBy 并且设置窗口
    val dmResult: DataStream[ProductVisitInfo] = browseLogDS.keyBy(info => {
      info.firstCategoryName + "$" + info.secondCategoryName + "$" + info.productName
    }).timeWindow(Time.seconds(10))
      .process(new ProcessWindowFunction[BrowseLogWideInfo, ProductVisitInfo, String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[BrowseLogWideInfo], out: Collector[ProductVisitInfo]): Unit = {
          //获取当前日期
          val currentDate: String = DateUtil.getDateYYYYMMDD(context.window.getStart.toString)
          //窗口开始时间
          val startTime: String = DateUtil.getDateYYYYMMDDHHMMSS(context.window.getStart.toString)
          //窗口结束时间
          val endTime: String = DateUtil.getDateYYYYMMDDHHMMSS(context.window.getEnd.toString)
          val arr: Array[String] = key.split("\\$")
          //商品的一级种类
          val firstCatName: String = arr(0)
          println("11111111111" + firstCatName)
          //商品的二级种类
          val secondCatName: String = arr(1)
          //商品的名称
          val productName: String = arr(2)
          //浏览商品次数
          val cnt: Long = elements.toList.size.toLong

          out.collect(ProductVisitInfo(currentDate, startTime, endTime, firstCatName, secondCatName, productName, cnt))

        }
      })

    //6.将 dmResult 写出到Clickhouse中
    /**
      *  clickhouse 表：本地表
      * create table dm_product_visit_info on cluster clickhouse_cluster_3shards_1replicas(
      * current_dt String,
      * window_start String,
      * window_end String,
      * first_cat String,
      * second_cat String,
      * product String,
      * product_cnt UInt32
      * ) engine = ReplicatedMergeTree('/clickhouse/tables/{shard}/dm_product_visit_info','{replica}') order by current_dt;
      *
      * 分片表
      * Create table dm_product_visit_info_all on cluster clickhouse_cluster_3shards_1replicas (
      * current_dt String,
      * window_start String,
      * window_end String,
      * first_cat String,
      * second_cat String,
      * product String,
      * product_cnt UInt32
      * )engine = Distributed(clickhouse_cluster_3shards_1replicas,default,dm_product_visit_info,product_cnt);
      */
    val insertSQL = "insert into dm_product_visit_info_all (current_dt,window_start,window_end,first_cat,second_cat,product,product_cnt)" +
      " values (?,?,?,?,?,?,?)"

    val cksink: SinkFunction[ProductVisitInfo] = MyClickhouseUtil.clickhouseSink[ProductVisitInfo](insertSQL, new JdbcStatementBuilder[ProductVisitInfo] {
      override def accept(pst: PreparedStatement, productVisitInfo: ProductVisitInfo): Unit = {
        pst.setString(1, productVisitInfo.currentDate)
        pst.setString(2, productVisitInfo.startTime)
        pst.setString(3, productVisitInfo.endTime)
        pst.setString(4, productVisitInfo.firstCatName)
        pst.setString(5, productVisitInfo.secondCatName)
        pst.setString(6, productVisitInfo.productName)
        pst.setLong(7, productVisitInfo.cnt)
      }
    })


    //7.将结果写出到Clickhouse中
    dmResult.addSink(cksink)

    env.execute()
  }

}
