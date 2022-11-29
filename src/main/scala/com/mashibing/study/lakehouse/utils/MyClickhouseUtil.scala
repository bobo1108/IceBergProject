package com.mashibing.study.lakehouse.utils

import java.sql.PreparedStatement

import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.sink.SinkFunction

/**
  * clickhouse工具类
  */
object MyClickhouseUtil {
  private val clickhouseDriver: String = ConfigUtil.CLICKHOUSE_DRIVER
  private val clickhouseUrl: String = ConfigUtil.CLICKHOUSE_URL
  private val clickhouseUser: String = ConfigUtil.CLICKHOUSE_USER
  private val clickhousePwd: String = ConfigUtil.CLICKHOUSE_PWD

  def clickhouseSink[T](insertSQL: String, builder: JdbcStatementBuilder[T] with Object {
    def accept(pst: PreparedStatement, info: T): Unit
  }): SinkFunction[T] = {
    JdbcSink.sink(
      //插入的SQL 语句
      insertSQL,
      //设置插入clickhouse 的参数
      builder,
      //设置向Clickhouse 批次插入数据参数
      new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
      //连接Clickhous的配置
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withDriverName(clickhouseDriver)
        .withUrl(clickhouseUrl)
        .withUsername(clickhouseUser)
        .withPassword(clickhousePwd).build())

  }

}
