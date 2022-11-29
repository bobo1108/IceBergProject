package com.mashibing.study.lakehouse.utils

object MyStringUtil {

  //判断字符串是否为空
  def isEmpty(str:String) = {
    str == null || "".equals(str)
  }
}
