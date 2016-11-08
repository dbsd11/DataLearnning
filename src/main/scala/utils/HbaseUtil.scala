package utils

import java.util.function.Consumer

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{Cell, HColumnDescriptor, HTableDescriptor, TableName}

/**
 * Created by bdiao on 16/11/7.
 */
object HbaseUtil {
  private var hbase: HBaseAdmin = null;

  def printlnCosumer = new Consumer[Cell]() {
    override def accept(t: Cell): Unit = {
      println(t)
      println(new String(t.getValue))
    }
  }

  def apply(hbase: HBaseAdmin): Unit = {
    this.hbase = hbase
  }

  def createTable(tableName: String, family: Array[String]): Unit = {
    if (isTableExist(tableName)) {
      return
    }

    var hdesc = new HTableDescriptor(TableName.valueOf(tableName))
    family.foreach(v => hdesc.addFamily(new HColumnDescriptor(v.getBytes())))
    hbase.createTable(hdesc)
  }

  def insert(rowKey: String, tableName: String, datas: Array[String]*): Unit = {
    if (!isTableExist(tableName)) {
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
    if (!isTableExist(tableName)) {
      return
    }
    var table = new HTable(TableName.valueOf(tableName), hbase.getConnection)
    var get = new Get(rowKey.getBytes())

    table.get(get).listCells().forEach(printlnCosumer)
  }

  def select(tableName: String): Unit = {
    if (!isTableExist(tableName)) {
      return
    }
    var table = new HTable(TableName.valueOf(tableName), hbase.getConnection)
    var rs: ResultScanner = null
    try {
      rs = table.getScanner(new Scan())
      rs.forEach(new Consumer[Result] {
        override def accept(t: Result): Unit = {
          t.listCells().forEach(printlnCosumer)
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      rs.close()
    }
  }

  def selectByStartStop(tableName: String, startRowKey: String, stopRowKey: String): Unit = {
    if (!isTableExist(tableName)) {
      return
    }
    var table = new HTable(TableName.valueOf(tableName), hbase.getConnection)
    var scan = new Scan()
    scan.setStartRow(startRowKey.getBytes())
    scan.setStopRow(stopRowKey.getBytes())
    var rs: ResultScanner = null
    try {
      rs = table.getScanner(scan)
      rs.forEach(new Consumer[Result] {
        override def accept(t: Result): Unit = {
          t.listCells().forEach(printlnCosumer)
        }
      })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      rs.close()
    }
  }

  def getByColumn(tableName: String, rowKey: String, familyName: String, columnName: String): Unit = {
    if (!isTableExist(tableName)) {
      return
    }
    var table = new HTable(TableName.valueOf(tableName), hbase.getConnection)
    var get = new Get(rowKey.getBytes())
    get.addColumn(familyName.getBytes(), columnName.getBytes())
    table.get(get).listCells().forEach(printlnCosumer)
  }

  def getByVersion(tableName: String, rowKey: String, familyName: String, columnName: String): Unit = {
    if (!isTableExist(tableName)) {
      return
    }
    var table = new HTable(TableName.valueOf(tableName), hbase.getConnection)
    var get = new Get(rowKey.getBytes())
    get.addColumn(familyName.getBytes, columnName.getBytes)
    get.setMaxVersions(5)
    table.get(get).listCells().forEach(printlnCosumer)
  }

  def update(tableName: String, rowKey: String, familyName: String, columnName: String, value: String): Unit = {
    if (!isTableExist(tableName)) {
      return
    }
    var table = new HTable(TableName.valueOf(tableName), hbase.getConnection)
    var put = new Put(rowKey.getBytes)
    put.add(familyName.getBytes, columnName.getBytes, value.getBytes)
    table.put(put)
  }

  def deleteColumn(tableName: String, rowKey: String, familyName: String, columnName: String): Unit = {
    if (!isTableExist(tableName)) {
      return
    }
    var table = new HTable(TableName.valueOf(tableName), hbase.getConnection)
    var delete = new Delete(rowKey.getBytes)
    if (columnName != null) {
      delete.addColumn(familyName.getBytes, columnName.getBytes);
    }
    table.delete(delete)
  }

  def dropTable(tableName: String): Unit = {
    if (!isTableExist(tableName)) {
      return
    }
    hbase.disableTable(tableName)
    hbase.deleteTable(tableName)
  }

  def isTableExist(tableName: String): Boolean = {
    if (hbase.tableExists(tableName)) {
      println("\n\ttable exist: " + tableName)
      return true;
    } else {
      println("\n\ttable not exist: " + tableName)
      return false
    }
  }
}