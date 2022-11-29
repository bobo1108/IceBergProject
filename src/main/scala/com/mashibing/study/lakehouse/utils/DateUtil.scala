package com.mashibing.study.lakehouse.utils

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Date

/**
  * 时间工具类
  */
object DateUtil {
  /**
    * 获取当前天日期 返回yyyy-MM-dd格式数据
    * @return
    */
  def getCurrentDateYYYYMMDD(): String = {
    val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val time: LocalDateTime = new Timestamp(new Date().getTime).toLocalDateTime
    time.format(dtf)
  }

  def getDateYYYYMMDDHHMMSS(tm: String) = {
    if (tm == null) {
      "1970-01-01 00:00:00"
    } else {
      val dft: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      val time: LocalDateTime = new Timestamp(tm.toLong).toLocalDateTime
      time.format(dft)
    }
  }

   //根据传进来的时间戳将数据转换成 yyyy-MM-dd 格式
  def getDateYYYYMMDD(tm: String) = {
    if(tm ==null){
      "1970-01-01"
    }else{
      val dft: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val time: LocalDateTime = new Timestamp(tm.toLong).toLocalDateTime
      time.format(dft)
    }
  }

}
