package com.mashibing.study.lakehouse.utils

import java.util

import com.alibaba.fastjson.JSONObject
import org.apache.flink.types.Row

object CommonUtil {
  //将row 类型数据转换成json
  def rowToJson(row: Row): JSONObject = {
    /**
      * lakehousedb,mc_member_info,insert,1648300465,10553,null,{gmt_create=1645019079851, balance=30804, user_id=uid756103,
      * member_points=9371, id=9997, gmt_modified=1645019079851, member_growth_score=4429, member_level=4}
      */
    val database = if(row.getField(0) == null) null else row.getField(0).toString
    val table = if(row.getField(1) == null) null else row.getField(1).toString
    val tp = if(row.getField(2) == null) null else row.getField(2).toString
    val ts = if(row.getField(3) == null) null else row.getField(3).toString
    val xid = if(row.getField(4) == null) null else row.getField(4).toString
    val commit = if(row.getField(5) == null) null else row.getField(5).toString
    val nObject = new JSONObject()
    nObject.put("database",database)
    nObject.put("table",table)
    nObject.put("tp",tp)
    nObject.put("ts",ts)
    nObject.put("xid",xid)
    nObject.put("commit",commit)

    if(row.getField(6) != null){
      val data: String = row.getField(6).toString
      val arr: Array[String] = data.stripPrefix("{").stripSuffix("}").split(",")
      for (elem <- arr) {
        if(elem.contains("=")&&elem.split("=").length==2){
          val splits: Array[String] = elem.split("=")
          nObject.put(splits(0).trim,splits(1).trim)
        }
      }

    }

    nObject
  }


  /**
    *  合并json数据
    */
  def AddAttributeToJson(jsonObj:JSONObject,addJson:JSONObject) = {
    import scala.collection.JavaConverters.asScalaSetConverter
    addJson.keySet().asScala.map(key=>{jsonObj.put(key,addJson.getString(key))})
  }
}
