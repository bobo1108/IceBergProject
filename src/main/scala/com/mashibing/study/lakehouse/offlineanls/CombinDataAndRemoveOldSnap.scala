package com.mashibing.study.lakehouse.offlineanls

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.flink.actions.Actions
import org.apache.iceberg.hadoop.HadoopCatalog

/**
  *  处理Iceberg中的小文件 ： 元数据小文件 + 数据文件
  */
object CombinDataAndRemoveOldSnap {
  def main(args: Array[String]): Unit = {

    val conf = new Configuration()

    val catalog = new HadoopCatalog(conf,"hdfs://hadoop102:9000/lakehousedata")

    //1.准备Iceberg表
    val table1: Table = catalog.loadTable(TableIdentifier.of("icebergdb","DWD_BROWSELOG"))
    val table2: Table = catalog.loadTable(TableIdentifier.of("icebergdb","DWD_USER_LOGIN"))
    val table3: Table = catalog.loadTable(TableIdentifier.of("icebergdb","DWS_BROWSE_INFO"))
    val table4: Table = catalog.loadTable(TableIdentifier.of("icebergdb","DWS_USER_LOGIN"))
    val table5: Table = catalog.loadTable(TableIdentifier.of("icebergdb","ODS_BROWSELOG"))
    val table6: Table = catalog.loadTable(TableIdentifier.of("icebergdb","ODS_MEMBER_ADDRESS"))
    val table7: Table = catalog.loadTable(TableIdentifier.of("icebergdb","ODS_MEMBER_INFO"))
    val table8: Table = catalog.loadTable(TableIdentifier.of("icebergdb","ODS_PRODUCT_CATEGORY"))
    val table9: Table = catalog.loadTable(TableIdentifier.of("icebergdb","ODS_PRODUCT_INFO"))
    val table10: Table = catalog.loadTable(TableIdentifier.of("icebergdb","ODS_USER_LOGIN"))

    //2.合并 parquet数据文件
    Actions.forTable(table1).rewriteDataFiles().execute()
    Actions.forTable(table2).rewriteDataFiles().execute()
    Actions.forTable(table3).rewriteDataFiles().execute()
    Actions.forTable(table4).rewriteDataFiles().execute()
    Actions.forTable(table5).rewriteDataFiles().execute()
    Actions.forTable(table6).rewriteDataFiles().execute()
    Actions.forTable(table7).rewriteDataFiles().execute()
    Actions.forTable(table8).rewriteDataFiles().execute()
    Actions.forTable(table9).rewriteDataFiles().execute()
    Actions.forTable(table10).rewriteDataFiles().execute()

    //3.删除历史快照
    table1.expireSnapshots().expireOlderThan(System.currentTimeMillis()).commit()
    table2.expireSnapshots().expireOlderThan(System.currentTimeMillis()).commit()
    table3.expireSnapshots().expireOlderThan(System.currentTimeMillis()).commit()
    table4.expireSnapshots().expireOlderThan(System.currentTimeMillis()).commit()
    table5.expireSnapshots().expireOlderThan(System.currentTimeMillis()).commit()
    table6.expireSnapshots().expireOlderThan(System.currentTimeMillis()).commit()
    table7.expireSnapshots().expireOlderThan(System.currentTimeMillis()).commit()
    table8.expireSnapshots().expireOlderThan(System.currentTimeMillis()).commit()
    table9.expireSnapshots().expireOlderThan(System.currentTimeMillis()).commit()
    table10.expireSnapshots().expireOlderThan(System.currentTimeMillis()).commit()

  }

}
