package com.mashibing.study.lakehouse.utils

object Beans {

  case class DwdInfo(iceberg_ods_tbl_name:String,kafka_dwd_topic:String,id:String,user_id1:String,ip:String,login_tm:String,logout_tm:String)

  case class UserLogin(userId:String,ip:String,loginTm:String,logoutTm:String)

  case class UserLoginWideInfo(user_id:String,ip:String,login_tm:String,logout_tm:String,member_growth_score:String,member_level:String,
                               member_points:String,balance:String,gmt_create:String,province:String,city:String,area:String,address:String,
                               phone_number:String,consignee_name:String)

  // BrowseLog 对象 —— 用户流浪商品的日志数据
  case class BrowseLog(logTime:String,userId:String,userIp:String,frontProductUrl:String,browseProductUrl:String,browseProductTpCode:String,
                       browseProductCode:String,obtainPoints:String)

  // BrowseLogWideInfo 对象 —— 用户浏览商品的宽表数据
  case class BrowseLogWideInfo(logTime:String,userId:String,userIp:String,productName:String,frontProductUrl:String,
                               browseProductUrl:String,firstCategoryName:String,secondCategoryName:String,obtainPoints:String)

  case class ProductVisitInfo(currentDate:String,startTime:String,endTime:String,firstCatName:String,secondCatName:String,productName:String,cnt:Long)
}
