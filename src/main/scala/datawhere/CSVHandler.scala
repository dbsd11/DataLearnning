package datawhere

import java.io._
import java.util.regex.Pattern

import scala.reflect.io.File

/**
 * Created by bdiao on 16/10/26.
 */
class CSVHandler {

  private var br: BufferedReader = null
  private var bw: BufferedWriter = null
  private var where_eq: Map[Int, String] = null
  private var where_high: Map[Int, String] = null
  private var where_low: Map[Int, String] = null
  private var lineFeed = '\n';
  private var commas = ','

  def apply(path: String) {
    apply (path, commas)
  }

  def apply(path: String, commas: Char): CSVHandler = {
    this.br = new BufferedReader(new InputStreamReader(new FileInputStream(path)))
    this.bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path + ".handout", true)))
    this.commas = commas;
    this
  }

  def handel(where: String): Unit = {

    if (where == null || !where.matches(CSVHandler.getWherePattern())) {
      return
    }
    where_eq = Map()
    where_high = Map()
    where_low = Map()

    var matcher = Pattern.compile(CSVHandler.conditionPattern).matcher(where)
    while (matcher.find()) {
      var condition = matcher.group()
      var operIndex = 2
      if (!CSVHandler.operator.contains(condition.charAt(operIndex))) {
        operIndex = 3;
      }
      if (operIndex != -1) {
        condition.charAt(operIndex) match {
          case '<' => where_high += condition.substring(1, operIndex).toInt -> condition.substring(operIndex + 1).replaceAll("\'", "")
          case '>' => where_low += condition.substring(1, operIndex).toInt -> condition.substring(operIndex + 1).replaceAll("\'", "")
          case '=' => where_eq += condition.substring(1, operIndex).toInt -> condition.substring(operIndex + 1).replaceAll("\'", "")
        }
      }
      operIndex = -1
      condition = null
    }
    matcher = null

    doSql()
  }

  def doSql(): Unit = {

    var line = br.readLine()
    var lineData: Array[String] = null
    while (line != null) {
      lineData = line.split(commas)
      if (where_eq.forall(entry => entry._2.compareTo(lineData.apply(entry._1)) == 0) &&
        where_high.forall(entry => entry._2.compareTo(lineData.apply(entry._1)) > 0) &&
        where_low.forall(entry => entry._2.compareTo(lineData.apply(entry._1)) < 0)) {
        bw.write(line + lineFeed)
        bw.flush()
      }
      line = br.readLine()
    }
    br.close()
    bw.close()
  }

}

object CSVHandler {
  private val operator = ">=<";
  private val conditionPattern = "\\$[0-9]*[>=<]'[^']+'";
  private val wherePattern = "^(\\s*where)?";
  private val commandPattern = "\\s*\\$[0-9]{1,2}[>=<]'[^']+'(\\s+and\\s+\\$[0-9]{1,2}[>=<]'[^']+')*\\s*"

  def getWherePattern(): String ={
    return CSVHandler.wherePattern + CSVHandler.commandPattern
  }

  def main(args: Array[String]) {
    val where = "where $0>'100000' and $2<'1000000'"
    File("bigdata").toDirectory.files.filter(_.name.matches(".*\\.depout-[0-9]+")).foreach(file=>{
      new CSVHandler()(file.path, ',').handel(where)
    })
  }
}
