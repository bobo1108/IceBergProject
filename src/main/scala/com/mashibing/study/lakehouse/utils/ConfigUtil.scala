package com.mashibing.study.lakehouse.utils

import com.typesafe.config.{Config, ConfigFactory}

/**
  * 读取配置的工具
  */
object ConfigUtil {

  val load: Config = ConfigFactory.load()

  //MySQLURL
  val MYSQL_URL: String = load.getString("mysql.url")
  val MYSQL_USER: String = load.getString("mysql.user")
  val MYSQL_PASSWORD: String = load.getString("mysql.password")

  //获取Kafka 集群
  val KAFKA_BROKERS: String = load.getString("kafka.cluster")

  //设置从头消费Kafka数据
  val CONSUMER_KAFKA_FROM_EARLIEST = load.getBoolean("consumer.kafka.from.earliest")

  //配置Kafka topic信息
  val KAFKA_DWD_USER_LOG_TOPIC :String = load.getString("kafka.dwd.userlog.topic")
  val KAFKA_DWD_BROWSE_LOG_TOPIC :String = load.getString("kafka.dwd.browselog.topic")
  val KAFKA_ODS_TOPIC :String = load.getString("kafka.ods.topic")
  val KAFKA_DIM_TOPIC :String = load.getString("kafka.dim.topic")
  val KAFKA_DWS_USER_LOGIN_WIDE_TOPIC :String = load.getString("kafka.dws.userlogin.wide.topic")
  val KAFKA_DWS_BROWSE_LOG_WIDE_TOPIC = load.getString("kafka.dws.browse.log.wide.topic")

  //phoenix 配置
  val PHOENIX_URL = load.getString("phoenix.url")


  //Hbase 维度数据
  val HBASE_DIM_MEMBER_INFO = load.getString("hbase.dim.member.info")
  val HBASE_DIM_MEMBER_ADDRESS_INFO = load.getString("hbase.dim.member.address.info")
  val HBASE_DIM_PRODUCT_INFO = load.getString("hbase.dim.product.info")
  val HBASE_DIM_PRODUCT_CATEGORY_INFO = load.getString("hbase.dim.product.category.info")

  //Redis 配置
  val REDIS_HOST = load.getString("redis.host")
  val REDIS_PORT = load.getInt("redis.port")
  val REDIS_DB = load.getInt("redis.db")
  val REDIS_TIMEOUT = load.getInt("redis.timeout")

  //clickhouse 配置
   val CLICKHOUSE_DRIVER = load.getString("clickhouse.driver")
   val CLICKHOUSE_URL = load.getString("clickhouse.url")
   val CLICKHOUSE_USER = load.getString("clickhouse.user")
   val CLICKHOUSE_PWD = load.getString("clickhouse.password")


}
