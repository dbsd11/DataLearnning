import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorSystem, Props}
import akka.routing.RoundRobinPool
import utils.LinesToMap

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
 * Created by bdiao on 16/11/3.
 */
final class TitanCodeTool extends LinesToMap {

  def this(path: String) {
    this()
    super.apply(path)
  }

  override def getData(tag: String, line: String): Map[String, String] = {
    var map: Map[String, String] = Map()
    val params = line.split(TitanCodeTool.codePattern)
    if (params.length == 2) {
      val len = params(0).length
      map += TitanCodeTool.feild -> tag
      map += TitanCodeTool.name -> params(0).substring(0, len - 1)
      map += TitanCodeTool.description -> params(1).replaceAll("[\"\\(\\),]", "")
      map += TitanCodeTool.code -> line.substring(len, len + 7)
    }
    map
  }
}

object TitanCodeTool {
  private val feild = "feild"
  private val name = "name"
  private val code = "code"
  private val description = "description"
  private val codePattern = "E[0-9]{6}"
}

class TestMain extends Actor {

  override def preStart() {
    if (TestMain.tool == null) {
      TestMain.init += 1
      TestMain.tool = new TitanCodeTool(TestMain.path)
    }
  }

  override def receive: Receive = {
    case "do" => {
      TestMain.call += 1
      TestMain.tool.read()
    }
  }
}

object TestMain {
  var init: Int = 0;
  var call: Int = 0;

  var path: String = "./bigdata/data"

  private var tool: LinesToMap = null

  def apply(): LinesToMap = {
    apply(path)
  }

  def apply(path: String): LinesToMap = {
    call += 1
    if (tool == null) {
      init += 1
      tool = new TitanCodeTool(path)
    }
    tool
  }

  def main(args: Array[String]) {
    val start = System.currentTimeMillis()
    val actorSystem = ActorSystem("local")
    val actor = actorSystem.actorOf(Props[TestMain].withRouter(RoundRobinPool(nrOfInstances = 100)));
    for (i <- 1 to 100) {
      //          TestMain().read()
      actor ! "do"
    }
    println(init + ":" + call + " time costs: " + (System.currentTimeMillis() - start))
  }
}