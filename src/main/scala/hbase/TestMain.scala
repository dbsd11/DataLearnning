package hbase

import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.HBaseAdmin
import utils.HbaseUtil

/**
 * Created by bdiao on 16/11/3.
 */
class TestMain {}

object TestMain {
  def main(args: Array[String]) {
    var conf = HBaseConfiguration.create()
    var admin: HBaseAdmin = new HBaseAdmin(conf)
    println(admin.getClusterStatus)

//    HbaseUtil(admin)
//
//    var tableName = "testtable"
//    var rowKey = "1"

    //      HbaseUtil.createTable(tableName,Array[String]("baseinfo","platforminfo"))

    //      println(admin.getTableRegions(TableName.valueOf("testtable")))

    //    HbaseUtil.insert(rowKey,tableName,Array[String]("name","age","userid"),Array[String]("dbsong刁必颂","24","asdassdd12321"), Array[String]("prikey"),Array[String]("121sadd123sda-100278"))

    //    HbaseUtil.selectByRowkey(rowKey, tableName);

    //    HbaseUtil.update(tableName, rowKey, "baseinfo", "name", "dbsong")

    //    HbaseUtil.selectByRowkey(rowKey, tableName);

    //    HbaseUtil.getByVersion(tableName, rowKey, "baseinfo", "name")

//    HbaseUtil.dropTable(tableName)
  }
}
