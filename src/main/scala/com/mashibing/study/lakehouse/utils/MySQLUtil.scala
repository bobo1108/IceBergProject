package com.mashibing.study.lakehouse.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.alibaba.fastjson.JSONObject
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

object MySQLUtil {
  //设置MySQL Source
  def getMySQLData(mysqlUrl: String, mysqlUser: String, mysqlPassword: String) = {

    new RichSourceFunction[JSONObject] {
      var flag = true
      var conn: Connection = _
      var pst: PreparedStatement = _

      //执行之前执行一次，用于初始化资源
      override def open(parameters: Configuration): Unit = {
        println("打开MySQL连接")
        conn = DriverManager.getConnection(mysqlUrl,mysqlUser,mysqlPassword)
        pst = conn.prepareStatement("select tbl_name,tbl_db,pk_col,cols,phoenix_tbl_name from dim_tbl_config_info")
      }

      //正常读取MySQL数据执行方法
      override def run(ctx: SourceFunction.SourceContext[JSONObject]): Unit = {
        while(flag){
          val rs: ResultSet = pst.executeQuery()
          while(rs.next()){
            val tblName: String = rs.getString("tbl_name")
            val tblDB: String = rs.getString("tbl_db")
            val pkCol: String = rs.getString("pk_col")
            val cols: String = rs.getString("cols")
            val phxTblName: String = rs.getString("phoenix_tbl_name")

            //创建返回的json对象
            val nObject = new JSONObject()
            nObject.put("tbl_db",tblDB)
            nObject.put("tbl_name",tblName)
            nObject.put("pk_col",pkCol)
            nObject.put("cols",cols)
            nObject.put("phoenix_tbl_name",phxTblName)
            ctx.collect(nObject)
          }
          rs.close()
          Thread.sleep(5*60*1000) //每隔5分钟读取MySQL 配置维度数据表
        }
      }

      //当取消任务执行方法，执行一次
      override def cancel(): Unit = {
        flag=false
        pst.close()
        conn.close()
      }

      //当程序停止时之前执行一次
      override def close(): Unit = {
        flag=false
        pst.close()
        conn.close()
      }
    }

  }


}
