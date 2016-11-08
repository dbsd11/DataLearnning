package utils

import org.apache.hadoop.hbase.{Cell, HColumnDescriptor, TableName, HTableDescriptor}
import org.apache.hadoop.hbase.client._

/**
 * Created by bdiao on 16/11/7.
 */
object HbaseUtil {
  private var hbase: HBaseAdmin = null;

  def apply(hbase: HBaseAdmin): Unit = {
    this.hbase = hbase
  }

  def createTable(tableName: String, family: Array[String]): Unit = {
    if(isTableExist(tableName)){
      return
    }

    var hdesc = new HTableDescriptor(TableName.valueOf(tableName))
    family.foreach(v => hdesc.addFamily(new HColumnDescriptor(v.getBytes())))
    hbase.createTable(hdesc)
  }

  def insert(rowKey: String, tableName: String, datas: Array[String]*): Unit = {
    if(!isTableExist(tableName)){
      return
    }
    var table = new HTable(TableName.valueOf(tableName), hbase.getConnection)
    var put = new Put(rowKey.getBytes())
    if (datas == null || datas.length % 2 != 0) {
      println("data fault: null or not_match")
      return
    }
    table.getTableDescriptor.getColumnFamilies.map(family => Pair(family.getName, table.getTableDescriptor.getColumnFamilies.indexOf(family))).foreach(familyPair => {
      var columns = datas(familyPair._2 * 2);
      var values = datas(familyPair._2 * 2 + 1);
      columns.map(column => Pair(column, columns.indexOf(column))).foreach(columnPair => put.addColumn(familyPair._1, columnPair._1.getBytes(), values(columnPair._2).getBytes()))
    })

    table.put(put)
  }

  def selectByRowkey(rowKey: String, tableName: String): Unit = {
    if(!isTableExist(tableName)){
      return
    }
    var table = new HTable(TableName.valueOf(tableName), hbase.getConnection)
    var get = new Get(rowKey.getBytes())
    for (cell <- table.get(get).listCells()) {
      println
    }

    def select(tableName: String): Unit ={
      if(!isTableExist(tableName)){
        return
      }
      var table = new HTable(TableName.valueOf(tableName), hbase.getConnection)
      var rs:ResultScanner=null
      try{
        rs = table.getScanner(new Scan())
        for(result:Result<-rs){
          for(cell<-result.listCells()){
            println
          }
        }
      }catch {
        case e:Exception => e.printStackTrace()
      }finally {
        rs.close()
      }
    }

    def selectByStartStop(tableName: String, startRowKey:String, stopRowKey:String): Unit ={
      if(!isTableExist(tableName)){
        return
      }
      var table = new HTable(TableName.valueOf(tableName), hbase.getConnection)
      var scan = new Scan()
      scan.setStartRow(startRowKey.getBytes())
      scan.setStopRow(stopRowKey.getBytes())
      var rs:ResultScanner=null
      try{
        rs = table.getScanner(scan)
        for(result:Result<-rs){
          for(cell<-result.listCells()){
            println
          }
        }
      }catch {
        case e:Exception => e.printStackTrace()
      }finally {
        rs.close()
      }
    }

    def getByColumn(tableName: String, rowKey:String, familyName:String, columnName:String): Unit ={
      if(!isTableExist(tableName)){
        return
      }
      var table = new HTable(TableName.valueOf(tableName), hbase.getConnection)
      var get = new Get(rowKey.getBytes())
      get.addColumn(familyName.getBytes(), columnName.getBytes())
      for(cell<-table.get(get)){
        println
      }
    }

    def getByVersion(tableName: String, rowKey:String, familyName:String, columnName:String): Unit ={
      if(!isTableExist(tableName)){
        return
      }
      var table = new HTable(TableName.valueOf(tableName), hbase.getConnection)
      var get = new Get(rowKey.getBytes())
      get.addColumn(familyName.getBytes, columnName.getBytes)
      get.setMaxVersions(5)
      for(cell<-table.get(get)){
        println
      }
    }

    def update(tableName: String, rowKey:String, familyName:String, columnName:String, value:String): Unit ={
      if(!isTableExist(tableName)){
        return
      }
      var table = new HTable(TableName.valueOf(tableName), hbase.getConnection)
      var put = new Put(rowKey.getBytes)
      put.add(familyName.getBytes, columnName.getBytes, value.getBytes)
      table.put(put)
    }

    def deleteColumn(tableName: String, rowKey:String, familyName:String, columnName:String): Unit ={
      if(!isTableExist(tableName)){
        return
      }
      var table = new HTable(TableName.valueOf(tableName), hbase.getConnection)
      var delete = new Delete(rowKey.getBytes)
      if(columnName!=null){
        delete.addColumn(familyName.getBytes, columnName.getBytes);
      }
      table.delete(delete)
    }

    def dropTable(tableName: String): Unit ={
      if(!isTableExist(tableName)){
        return
      }
      hbase.deleteTable(tableName)
    }

  }

  def isTableExist(tableName:String): Boolean ={
    if (hbase.tableExists(tableName)) {
      println("table exist: "+tableName)
      return true;
    }else{
      println("table not exist: "+tableName)
      return false
    }
  }
}
