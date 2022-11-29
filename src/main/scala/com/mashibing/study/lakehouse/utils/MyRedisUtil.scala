package com.mashibing.study.lakehouse.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * Redis 工具类
  */
object MyRedisUtil {
  private val redisHost: String = ConfigUtil.REDIS_HOST
  private val redisPort: Int = ConfigUtil.REDIS_PORT
  private val redisDB: Int = ConfigUtil.REDIS_DB
  private val redisTimeOut: Int = ConfigUtil.REDIS_TIMEOUT

  //准备Redis 连接池,保证线程安全和查询效率
  lazy private val pool = new JedisPool(new GenericObjectPoolConfig(),redisHost,redisPort,redisTimeOut)


  /**
    * 从Redis中获取缓存数据
    * Redis中存储数据格式 :(维度表名-维度key,json维度数据)
    */
  def getInfoFromRedisCache(hbaseDimTbl: String, id: String): String = {
    //获取redis 连接
    val jedis: Jedis = MyRedisUtil.pool.getResource
    jedis.select(redisDB)
    val jsonStr:String =jedis.get(s"$hbaseDimTbl-$id")
    MyRedisUtil.pool.returnResource(jedis)
    jsonStr
  }

  /**
    * 向 Redis中设置对应的查询的维度数据
    */
  def setRedisDimCache(hbaseDimTbl: String, key: String, jsonStr: String): Unit = {
    //获取redis 连接
    val jedis: Jedis = MyRedisUtil.pool.getResource
    jedis.select(redisDB)
    //设置key 过期的时间自动删除Redis缓存数据 ，24小时
    jedis.setex(hbaseDimTbl+"-"+key,60*60*24,jsonStr)
    MyRedisUtil.pool.returnResource(jedis)
  }



}
