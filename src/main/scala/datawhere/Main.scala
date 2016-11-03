package datawhere

import scala.reflect.io.File

/**
 * Created by bdiao on 16/10/30.
 */
object Main {
  var home:String = null
  var wordDeli = ','
  var dist = 20
  var core = 4

  def main(args: Array[String]) {
    if (args.length == 0) {
      showUseage()
    } else {
      init(args)
    }
    var data = File(args(0))

    new FileDeparter()(data.path, dist, core).startJob()

    data.parent.files.filter(_.name.matches(".*\\.depout-[0-9]+")).foreach(file=>{
      new CSVHandler()(file.path, wordDeli).handel(args(1))
    })

  }

  def showUseage(): Unit = {
    println("args:  DataFilePath WhereClause WordDelimiter BlockNum JobCoreSize")
    println("\teg:  data.csv  \"$0>'1' and $2='2'\"")
    println("\teg:  data.csv  \"$0>'1' and $2='2'\" ,")
    println("\teg:  data.csv  \"$0>'1' and $2='2'\" , 20  4")
    println("\n\twhereClause regex:  "+CSVHandler.getWherePattern())
    System.exit(-1)
  }

  def init(args: Array[String]): Unit = {
    home = File(args(0)).toDirectory.path

    if (args.length > 2) {
      wordDeli = args(2).trim.charAt(0)
    }
    if (args.length > 3) {
      dist = args(3).toInt
    }
    if (args.length > 4) {
      core = args(4).toInt
    }
  }

}
