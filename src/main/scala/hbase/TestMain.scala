package hbase

import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.HBaseAdmin

/**
 * Created by bdiao on 16/11/3.
 */
class TestMain {}

object TestMain{
  def main(args: Array[String]) {
      var conf = HBaseConfiguration.create()
      var admin:HBaseAdmin = new HBaseAdmin(conf)
      println(admin.getClusterStatus)
      println(admin.getTableRegions(TableName.valueOf("*")))
  }
}
