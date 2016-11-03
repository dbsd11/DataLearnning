package utils

import java.io.BufferedReader

import scala.io.Codec
import scala.reflect.io.File
import scala.util.control.Breaks

/**
 * Created by bdiao on 16/11/2.
 */
abstract class LinesToMap {
  private var path: String = null

  def apply(path: String): Unit = {
    this.path = path
  }

  def read(): String = {
    val br = new BufferedReader(File(path).reader(Codec("utf-8")))
    var datas: List[Map[String, String]] = List()
    var str = br.readLine()
    while (str != null) {
      if (!str.matches(LinesToMap.tagPattern)) {
        str = br.readLine()
      } else {
        val tag = str.trim.replaceAll("\\[\\]", "")
        val loop = new Breaks();
        loop.breakable {
          str = br.readLine()
          while (str != null) {
            if (str.matches(LinesToMap.tagPattern)) {
              loop.break()
            }
            str = str.trim
            datas = datas.::(getData(tag, str))
            str = br.readLine()
          }
        }
      }
    }
    br.close()
    return makeJSON(datas)
  }

  def getData(tag: String, line: String): Map[String, String]

  private def makeJSON(a: Any): String = a match {
    case m: Map[String, Any] => m.map {
      case (name, content) => "\"" + name + "\":" + makeJSON(content)
    }.mkString("{", ",", "}")
    case l: List[Any] => l.map(makeJSON).mkString("[", ",", "]")
    case s: String => "\"" + s + "\""
    case i: Int => i.toString
  }
}

object LinesToMap {
  private val tagPattern = "\\s*\\[.+\\]\\s*"
  private val codePattern = "E[0-9]{6}"
}

