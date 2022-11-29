package com.mashibing.study.lakehouse.dws

import java.lang
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.mashibing.study.lakehouse.utils.Beans.{UserLogin, UserLoginWideInfo}
import com.mashibing.study.lakehouse.utils.{CommonUtil, ConfigUtil, MyRedisUtil, MyStringUtil}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import com.alibaba.fastjson.JSON
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord
/**
  *  处理用户登录数据与phx中的维度数据进行关联，得到用户主题宽表数据
  *  1.读取Kafka “” topic 中实时数据与Phoenix中维度数据表进行关联
  *  2.使用Redis做缓存
  *  3.将关联宽表数据写入到Iceberg-DWS层
  *  4.将关联宽表数据写入到 Kafka 对应的topic
  */
object ProcessUserlogInToDWS {
  private val hbaseDimMemberInfoTbl = ConfigUtil.HBASE_DIM_MEMBER_INFO
  private val hbaseDimMemberAddressInfoTbl = ConfigUtil.HBASE_DIM_MEMBER_ADDRESS_INFO
  private val kafkaDwsUserLoginWideTopic = ConfigUtil.KAFKA_DWS_USER_LOGIN_WIDE_TOPIC
  private val kafkaBrokers = ConfigUtil.KAFKA_BROKERS

  def main(args: Array[String]): Unit = {

    //1.准备环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tblEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    import org.apache.flink.streaming.api.scala._

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
    /**
      * {
      * "database": "lakehousedb",
      * "xid": "20566",
      * "user_id": "uid185880",
      * "ip": "54.223.109.158",
      * "id": "9992",
      * "tp": "insert",
      * "logout_tm": "2022-03-08 13:36:09",
      * "kafka_dwd_topic": "KAFKA-DWD-USER-LOGIN-TOPIC",
      * "table": "mc_user_login",
      * "ts": "1648300467",
      * "login_tm": "2022-03-08 12:03:06"
      * }
      */
    tblEnv.executeSql(
      """
        | create table kafka_dwd_user_login_tbl(
        |   id string,
        |   user_id string,
        |   ip string,
        |   login_tm string,
        |   logout_tm string
        | ) with (
        |   'connector'='kafka',
        |   'topic'='KAFKA-DWD-USER-LOGIN-TOPIC',
        |   'properties.bootstrap.servers'='hadoop102:9092,hadoop103:9092,hadoop104:9092',
        |   'properties.group.id'='mygroup-id',
        |   'scan.startup.mode'='earliest-offset',--latest-offset
        |   'format'='json'
        | )
      """.stripMargin)

    val table :Table = tblEnv.sqlQuery(
      """
        |select user_id,ip,login_tm,logout_tm from kafka_dwd_user_login_tbl
      """.stripMargin)

    //4.将Row 类型的DataStream转换成对象类型的DataStream ,方便后续操作
    val userLoginDS: DataStream[UserLogin] = tblEnv.toAppendStream[Row](table).map(row => {
      val userID: String = row.getField(0).toString
      val ip: String = row.getField(1).toString
      val loginTm: String = row.getField(2).toString
      val logoutTm: String = row.getField(3).toString
      UserLogin(userID, ip, loginTm, logoutTm)
    })

    //定义侧输出Tag
    val kafkaDataTag = new OutputTag[JSONObject]("kafka_data")

    //5.连接Phx 根据 userID 获取 DIM_MEMBER_INFO-用户基本信息 和 DIM_MEMBER_ADDRESS-用户收获地址信息，这里利用Redis做缓存
    val wideUserLoginDS: DataStream[UserLoginWideInfo] = userLoginDS.process(new ProcessFunction[UserLogin,UserLoginWideInfo] {
      var conn: Connection = _
      var pst : PreparedStatement = _
      var rs : ResultSet = _

      // 连接phx
      override def open(parameters: Configuration): Unit = {
        println("连接Phoenix ... ...")
        conn = DriverManager.getConnection(ConfigUtil.PHOENIX_URL)
      }

      //流中有一条数据就会处理一次
      override def processElement(userLogin: UserLogin, ctx: ProcessFunction[UserLogin, UserLoginWideInfo]#Context, out: Collector[UserLoginWideInfo]): Unit = {
        //5.1 准备最终结果的json 对象,先把这条数据内容准备到json对象中
        val jsonObj = new JSONObject()
        jsonObj.put("user_id",userLogin.userId)
        jsonObj.put("ip",userLogin.ip)
        jsonObj.put("login_tm",userLogin.loginTm)
        jsonObj.put("logout_tm",userLogin.logoutTm)

        //5.2 根据 userId 从Redis中获取 DIM_MEMBER_INFO-用户基本信息 数据
        val redisCacheMemberInfo :String = MyRedisUtil.getInfoFromRedisCache(hbaseDimMemberInfoTbl,userLogin.userId)

        //5.3 根据 userId 从Redis中获取 DIM_MEMBER_ADDRESS-用户收货地址信息
        val redisCacheAddressInfo :String = MyRedisUtil.getInfoFromRedisCache(hbaseDimMemberAddressInfoTbl,userLogin.userId)

        //5.4 redisCacheMemberInfo 如果为空那么就从Phx中查询，非空，那么就转换成json 加入到 jsonObj 中
        if(MyStringUtil.isEmpty(redisCacheMemberInfo)){
          //为空，从phx中查询
          println("连接phx 查询 DIM_MEMBER_INFO - 用户基本信息表 维度数据")
          val sql =
            s"""
              | select user_id,member_growth_score,member_level,member_points,balance,gmt_create,gmt_modified
              |  from DIM_MEMBER_INFO where user_id = '${userLogin.userId}'
            """.stripMargin

          pst = conn.prepareStatement(sql)
          rs = pst.executeQuery()

          //准备 向Redis中缓存数据的json 对象
          val dimMemberInfoRedisJsonObj = new JSONObject()
          while(rs.next()){
            dimMemberInfoRedisJsonObj.put("member_growth_score",rs.getString("member_growth_score"))
            dimMemberInfoRedisJsonObj.put("member_level",rs.getString("member_level"))
            dimMemberInfoRedisJsonObj.put("member_points",rs.getString("member_points"))
            dimMemberInfoRedisJsonObj.put("balance",rs.getString("balance"))
            dimMemberInfoRedisJsonObj.put("gmt_create",rs.getString("gmt_create"))
            dimMemberInfoRedisJsonObj.put("gmt_modified",rs.getString("gmt_modified"))

            //将当前从phx中查询出来的数据要缓存到redis
            MyRedisUtil.setRedisDimCache(hbaseDimMemberInfoTbl,userLogin.userId,dimMemberInfoRedisJsonObj.toString)

            //将当前查询出来的json对象合并到 jsonObj
            CommonUtil.AddAttributeToJson(jsonObj,dimMemberInfoRedisJsonObj)
          }
        }else{
          //非空，直接加入到最终的 jsonObj 中
          println("从Redis 缓存中获取 DIM_MEMBER_INFO - 用户基本信息表 维度数据")
          CommonUtil.AddAttributeToJson(jsonObj,JSON.parseObject(redisCacheMemberInfo))
        }

        //5.5 redisCacheAddressInfo 如果为空那么就从Phx中查询，非空，那么就转换成json 加入到 jsonObj 中
        if(MyStringUtil.isEmpty(redisCacheAddressInfo)){
         //查询为空，从phx中查询对应的收货地址数据
          println("连接phx查询 DIM_MEMBER_ADDRESS -用户收货地址信息表 维度数据")
          val sql =
            s"""
              | select user_id,province,city,area,address,lon,lat,phone_number,consignee_name
              | from DIM_MEMBER_ADDRESS where user_id = '${userLogin.userId}'
            """.stripMargin

          pst = conn.prepareStatement(sql)
          rs = pst.executeQuery()

          //准备 向Redis中缓存数据的json 对象
          val dimMemberAddressInfoRedisJsonObj = new JSONObject()
          while(rs.next()){
            dimMemberAddressInfoRedisJsonObj.put("province",rs.getString("province"))
            dimMemberAddressInfoRedisJsonObj.put("city",rs.getString("city"))
            dimMemberAddressInfoRedisJsonObj.put("area",rs.getString("area"))
            dimMemberAddressInfoRedisJsonObj.put("address",rs.getString("address"))
            dimMemberAddressInfoRedisJsonObj.put("phone_number",rs.getString("phone_number"))
            dimMemberAddressInfoRedisJsonObj.put("consignee_name",rs.getString("consignee_name"))

            //将当前从phx中查询出来的数据要缓存到redis
            MyRedisUtil.setRedisDimCache(hbaseDimMemberAddressInfoTbl,userLogin.userId,dimMemberAddressInfoRedisJsonObj.toString)

            //将当前查询出来的json对象合并到 jsonObj
            CommonUtil.AddAttributeToJson(jsonObj,dimMemberAddressInfoRedisJsonObj)
          }

        }else{
          //非空，直接加入到最终的 jsonObj中
          println("从Redis 缓存中获取 DIM_MEMBER_ADDRESS-用户收货地址信息表 维度数据")
          CommonUtil.AddAttributeToJson(jsonObj,JSON.parseObject(redisCacheAddressInfo))
        }

        //5.6 将 jsonObj 写入到 Flink 侧输出流中，方便后期从侧流中获取数据写入Kafka
        ctx.output(kafkaDataTag ,jsonObj)

        //5.7 将 jsonObj 中的数据封装对象返回
        /**
          * user_id:String,ip:String,login_tm:String,logout_time:String,member_growth_score:String,member_level:String,
          * member_points:String,balance:String,gmt_create:String,province:String,city:String,area:String,address:String,
          * phone_number:String,consignee_name:String
          */
        out.collect(UserLoginWideInfo(jsonObj.getString("user_id"),jsonObj.getString("ip"),jsonObj.getString("login_tm"),
          jsonObj.getString("logout_tm"),jsonObj.getString("member_growth_score"),jsonObj.getString("member_level"),
          jsonObj.getString("member_points"),jsonObj.getString("balance"),jsonObj.getString("gmt_create"),
          jsonObj.getString("province"),jsonObj.getString("city"),jsonObj.getString("area"),jsonObj.getString("address"),
          jsonObj.getString("phone_number"),jsonObj.getString("consignee_name")))
      }

      override def close(): Unit ={
        rs.close()
        pst.close()
        conn.close()
      }
    })

    //6.将结果数据转换成Table 写入到 Iceberg - DWS 层：DWS_USER_LOGIN
    val wideTbl: Table = tblEnv.fromDataStream(wideUserLoginDS)
    wideTbl.printSchema()
    tblEnv.executeSql(
      s"""
        | insert into hadoop_iceberg.icebergdb.DWS_USER_LOGIN
        | select
        |   user_id,
        |   ip,
        |   gmt_create,
        |   login_tm,
        |   logout_tm,
        |   member_level,
        |   province,
        |   city,
        |   area,
        |   address,
        |   member_points,
        |   balance,
        |   member_growth_score
        | from ${wideTbl}
      """.stripMargin)

    //7.将最终结果侧流中的数据写入到Kafka
    val props = new Properties()
    props.setProperty("bootstrap.servers",kafkaBrokers)
    wideUserLoginDS.getSideOutput(kafkaDataTag).print()
//    wideUserLoginDS.getSideOutput(kafkaDataTag).addSink(new FlinkKafkaProducer[JSONObject](kafkaDwsUserLoginWideTopic,
//      new KafkaSerializationSchema[JSONObject] {
//      override def serialize(jsonObj: JSONObject, timestamp: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
//        new ProducerRecord[Array[Byte], Array[Byte]](kafkaDwsUserLoginWideTopic,null,jsonObj.toString.getBytes())
//      }
//    },props,FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))

    env.execute()
  }

}
