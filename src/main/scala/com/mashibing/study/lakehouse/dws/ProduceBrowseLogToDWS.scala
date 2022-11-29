package com.mashibing.study.lakehouse.dws

import java.lang
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.JSON
import com.mashibing.study.lakehouse.utils.Beans.{BrowseLog, BrowseLogWideInfo}
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
  *  读取 Kafka - DWD - KAFKA-DWD-BROWSE-LOG-TOPIC 数据
  *  结合 Phoenix 中维度数据 得到 用户浏览商品的宽表数据
  */
object ProduceBrowseLogToDWS {
  private val hbaseDimProductInfo: String = ConfigUtil.HBASE_DIM_PRODUCT_INFO
  private val hbaseDimProductCategoryInfo: String = ConfigUtil.HBASE_DIM_PRODUCT_CATEGORY_INFO
  private val kafkaBrokers: String = ConfigUtil.KAFKA_BROKERS
  private val phxUrl: String = ConfigUtil.PHOENIX_URL
  private val kafkaDwsBrowseLogWideTopic: String = ConfigUtil.KAFKA_DWS_BROWSE_LOG_WIDE_TOPIC

  def main(args: Array[String]): Unit = {
    //1.创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tblEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    //2.设置checkpoint
    env.enableCheckpointing(5000)

    //3.导入隐式转换
    import org.apache.flink.streaming.api.scala._

    //4.创建iceberg对应的 catalog
    tblEnv.executeSql(
      """
        |create catalog hadoop_iceberg with(
        | 'type'='iceberg',
        | 'catalog-type'='hadoop',
        | 'warehouse'='hdfs://hadoop102:9000/lakehousedata'
        |)
      """.stripMargin)

    //5.flink kafka connector 方式读取Kafka -DWD - KAFKA-DWD-BROWSE-LOG-TOPIC 数据
    tblEnv.executeSql(
      """
        | create table kafka_dwd_browse_log_tbl(
        |   logTime string,
        |   userId string,
        |   userIp string,
        |   frontProductUrl string,
        |   browseProductUrl string,
        |   browseProductTpCode string,
        |   browseProductCode string,
        |   obtainPoints string
        | ) with (
        |  'connector' = 'kafka',
        |  'topic' = 'KAFKA-DWD-BROWSE-LOG-TOPIC',
        |  'properties.bootstrap.servers'='hadoop102:9092,hadoop103:9092,hadoop104:9092',
        |  'scan.startup.mode' = 'earliest-offset', -- earliest-offset
        |  'properties.group.id' = 'mygrourpid',
        |  'format'='json'
        | )
      """.stripMargin)

    val browseLogTbl:Table = tblEnv.sqlQuery(
      """
        | select
        | logTime,userId,userIp,frontProductUrl,browseProductUrl,browseProductTpCode,browseProductCode,obtainPoints
        |  from kafka_dwd_browse_log_tbl
      """.stripMargin)

    //6. 将读取过来的数据转换成对象类型的DataStream，方便后期操作
    val browseLogDS: DataStream[BrowseLog] = tblEnv.toAppendStream[Row](browseLogTbl).map(row => {
      val logTime: String = row.getField(0).toString
      val userId: String = row.getField(1).toString
      val userIp: String = row.getField(2).toString
      val frontProductUrl: String = row.getField(3).toString
      val browseProductUrl: String = row.getField(4).toString
      val browseProductTpCode: String = row.getField(5).toString // 浏览商品的二级分类code
      val browseProductCode: String = row.getField(6).toString //浏览商品编号
      val obtainPoints: String = row.getField(7).toString
      BrowseLog(logTime, userId, userIp, frontProductUrl, browseProductUrl, browseProductTpCode, browseProductCode, obtainPoints)
    })

    //定义 侧流标记
    val kafkaDataTag = new OutputTag[JSONObject]("kafkaDataTag")

    //7. 连接phx 获取维度数据，维度数据有两类：DIM_PRODUCT_INFO 、DIM_PRODUCT_CATEGORY
    val browseLogWideInfoDS: DataStream[BrowseLogWideInfo]  = browseLogDS.process(new ProcessFunction[BrowseLog,BrowseLogWideInfo]() {

      var conn: Connection = _
      var pst: PreparedStatement = _

      //open 方法只执行一次，用于创建配置
      override def open(parameters: Configuration): Unit = {
         conn = DriverManager.getConnection(phxUrl)
      }

      override def processElement(browseLog: BrowseLog, ctx: ProcessFunction[BrowseLog, BrowseLogWideInfo]#Context, out: Collector[BrowseLogWideInfo]): Unit = {
        //准备最终返回的 json 对象
        val jsonObj = new JSONObject()
        jsonObj.put("log_time",browseLog.logTime)
        jsonObj.put("user_id",browseLog.userId)
        jsonObj.put("user_ip",browseLog.userIp)
        jsonObj.put("front_product_url",browseLog.frontProductUrl)
        jsonObj.put("browse_product_url",browseLog.browseProductUrl)
        jsonObj.put("browse_product_tpcode",browseLog.browseProductTpCode) //浏览商品的二级分类id
        jsonObj.put("browse_product_code",browseLog.browseProductCode) //浏览商品的id
        jsonObj.put("obtain_points",browseLog.obtainPoints)

        /**
          * 1.根据浏览商品id - browseProductCode ，从Redis 缓存中获取 对应的商品名称
          *    如果Redis 缓存中没有对应的商品名称，从Phx 维度表 - DIM_PRODUCT_INFO 中获取商品的名称，获取后还要向Redis缓存中设置数据，查询数据结果写入jsonObj中
          *    如果Redis 缓存中有对应的数据，获取缓存数据将结果写入 jsonObj中
          */
        val productInfoRedisCacheInfo: String = MyRedisUtil.getInfoFromRedisCache(hbaseDimProductInfo,browseLog.browseProductCode)

        if(MyStringUtil.isEmpty(productInfoRedisCacheInfo)){
          //Redis中没没有缓存数据 ，就从Phx中查询
          println("从 Phx-DIM_PRODUCT_INFO 表中查询对应的商品基本信息 ")
          val sql =
            s"""
              | select product_id,product_name from DIM_PRODUCT_INFO where PRODUCT_ID = '${browseLog.browseProductCode}'
            """.stripMargin
          pst = conn.prepareStatement(sql)
          val rs: ResultSet = pst.executeQuery()
          val dimProductInfoRedisJsonObj = new JSONObject()

          while(rs.next()){
            dimProductInfoRedisJsonObj.put("product_id",rs.getString("product_id"))
            dimProductInfoRedisJsonObj.put("product_name",rs.getString("product_name"))
          }

          //将对应的查询结果写入到Redis缓存中
          MyRedisUtil.setRedisDimCache(hbaseDimProductInfo,browseLog.browseProductCode,dimProductInfoRedisJsonObj.toString)

          //将获取的最后结果加入到最终的 jsonObj 中
          CommonUtil.AddAttributeToJson(jsonObj,dimProductInfoRedisJsonObj)

        }else{
          //Redis中有缓存数据
          println("DIM_PRODUCT_INFO - 商品基本信息表 ：从Redis中获取对应的缓存数据")
          CommonUtil.AddAttributeToJson(jsonObj,JSON.parseObject(productInfoRedisCacheInfo))
        }

        /**
          * 2.根据浏览商品二级分类code  - browseProductTpCode ,从Redis缓存中获取对应的商品的一级、二级分类
          *    如果Redis 缓存中没有对应的商品级别分类信息，从phx 维度表 - DIM_PRODUCT_CATEGORY 中获取商品的一级、二级分类
          *           获取后，要向Redis中设置缓存，并且将结果写出到jsonObj中
          *
          *    如果Redis 缓存中有对应的数据，获取缓存数据将结果写入jsonObj中
          */
        val productCategroyRedisCacheInfo: String = MyRedisUtil.getInfoFromRedisCache(hbaseDimProductCategoryInfo,browseLog.browseProductTpCode)

        if(MyStringUtil.isEmpty(productCategroyRedisCacheInfo)){
          //Redis中没有缓存的一级、二级分类数据
          println("从 Phx-DIM_PRODUCT_CATEGORY 表中查询对应的商品类别信息 ")
          val sql =
            s"""
              |select
              | b.id as first_category_id,
              | b.name as first_category_name,
              | a.id as second_category_id,
              | a.name as second_category_name
              | from  DIM_PRODUCT_CATEGORY a join  DIM_PRODUCT_CATEGORY b on a.p_id = b.id
              | where a.id = '${browseLog.browseProductTpCode}'
            """.stripMargin

          pst = conn.prepareStatement(sql)
          val rs: ResultSet = pst.executeQuery()

          val dimProductCategoryRedisJsonObj = new JSONObject()
          while(rs.next()){
            dimProductCategoryRedisJsonObj.put("first_category_id",rs.getString("first_category_id"))
            dimProductCategoryRedisJsonObj.put("first_category_name",rs.getString("first_category_name"))
            dimProductCategoryRedisJsonObj.put("second_category_id",rs.getString("second_category_id"))
            dimProductCategoryRedisJsonObj.put("second_category_name",rs.getString("second_category_name"))
          }

          //将 dimProductCategoryRedisJsonObj 写入到Redis缓存中
          MyRedisUtil.setRedisDimCache(hbaseDimProductCategoryInfo,browseLog.browseProductTpCode,dimProductCategoryRedisJsonObj.toString)

          //将结果写入到最终的 jsonObj
          CommonUtil.AddAttributeToJson(jsonObj,dimProductCategoryRedisJsonObj)

        }else{
          //redis中有缓存商品级别的数据
          println("DIM_PRODUCT_CATEGORY - 商品类别信息表 ：从Redis中获取对应的缓存数据")
          CommonUtil.AddAttributeToJson(jsonObj,JSON.parseObject(productCategroyRedisCacheInfo))
        }

        //将 jsonObj 写出到 侧流中
        ctx.output(kafkaDataTag,jsonObj)

        //将最后的 jsonObj 写入对应的对象中返回到主流

        out.collect(BrowseLogWideInfo(
          jsonObj.getString("log_time").split(" ")(0),
          jsonObj.getString("user_id"),
          jsonObj.getString("user_ip"),
          jsonObj.getString("product_name"),
          jsonObj.getString("front_product_url"),
          jsonObj.getString("browse_product_url"),
          jsonObj.getString("first_category_name"),
          jsonObj.getString("second_category_name"),
          jsonObj.getString("obtain_points")
        ))

      }

      override def close(): Unit = {
        pst.close()
        conn.close()
      }
    })
//
//    browseLogWideInfoDS.print("主流：")
//    browseLogWideInfoDS.getSideOutput(kafkaDataTag).print("侧流：")


    //8.将宽表数据组织成 Table 对象，结果写入Iceberg-DWS层
//    val table: Table = tblEnv.fromDataStream(browseLogWideInfoDS)
//    table.printSchema()
//    tblEnv.executeSql(
//      s"""
//        | insert into hadoop_iceberg.icebergdb.DWS_BROWSE_INFO
//        | select
//        |  logTime,
//        |  userId,
//        |  userIp,
//        |  productName,
//        |  frontProductUrl,
//        |  browseProductUrl,
//        |  firstCategoryName,
//        |  secondCategoryName,
//        |  obtainPoints
//        | from ${table}
//      """.stripMargin)


    //9.将侧流中的数据写入到Kafka KAFKA-DWS-BROWSE-LOG-WIDE-TOPIC - 用户浏览商品宽表数据 中
    val props = new Properties()
    props.setProperty("bootstrap.servers",kafkaBrokers)

    browseLogWideInfoDS.getSideOutput(kafkaDataTag).addSink(new FlinkKafkaProducer[JSONObject](kafkaDwsBrowseLogWideTopic,
      new KafkaSerializationSchema[JSONObject] {
      override def serialize(jsonObj: JSONObject, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
        new ProducerRecord[Array[Byte], Array[Byte]](kafkaDwsBrowseLogWideTopic,null,jsonObj.toString.getBytes())
      }
    },props,FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))

    env.execute()
  }

}
