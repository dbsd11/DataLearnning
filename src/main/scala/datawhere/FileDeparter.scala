package datawhere

import java.io.{File, _}
import java.nio.ByteBuffer
import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}

import akka.actor._
import akka.routing.RoundRobinPool

import scala.util.Random

/**
 * Created by bdiao on 16/10/27.
 */
class FileDeparter {
  private var dataFile: File = null
  private var core = 4
  private var br: BufferedReader = null
  private var dist = 20
  private var bucket: Array[OutputStream] = null
  private var threadPool: ThreadPoolExecutor = null

  private var sender: ActorRef = null
  private var omitFile: File = null
  private var dh1 = Integer.parseUnsignedInt("10000000", 2).toInt
  private var dh2 = Integer.parseUnsignedInt("11000000", 2).toInt
  private var dh3 = Integer.parseUnsignedInt("11100000", 2).toInt
  private var lineSep = 10

  def apply(path: String): FileDeparter = {
    apply(path, dist)
  }

  def apply(path: String, dist: Int): FileDeparter = {
    apply(path, dist, core)
  }

  def apply(path: String, dist: Int, core: Int): FileDeparter = {

    this.dataFile = new File(path)
    this.omitFile = new File(path + ".omit")
    this.core = core
    this.dist = dist
    br = new BufferedReader(new InputStreamReader(new FileInputStream(path), "utf-8"))
    bucket = new Array[OutputStream](dist)
    for (i <- 1 to dist) {
      bucket.update(i - 1, new FileOutputStream(dataFile.getPath + ".depout-" + i, true))
    }
    threadPool = new ThreadPoolExecutor(core, 100, 1, TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](100));
    this
  }

  private def doDepart(): Unit = {

    var offset = (dataFile.length + dist - 1) / dist

    for (i <- 1 to dist) {
      threadPool.execute(new Runnable {
        override def run(): Unit = {
          try {
            var file: RandomAccessFile = new RandomAccessFile(dataFile.getPath, "rw");
            var head: Long = (i - 1) * offset;
            var limit: Long = Math.min(i * offset, dataFile.length);
            var tail = limit
            file.seek(head)
            var bb = ByteBuffer.allocate(1024)
            var b: Byte = ' '
            do {
              b = file.readByte()
              bb.put(b)
              head += 1;
            } while (b != lineSep && head != tail)

            file.seek(tail - 1)
            b = file.readByte()
            while (b != lineSep && tail != head) {
              tail -= 1
              file.seek(tail - 1)
              b = file.readByte()
            }
            file.seek(tail)
            for (i <- 1 to (limit - tail).toInt) {
              b = file.readByte()
              bb.put(b)
            }
            bb.flip()
            sender ! Put(i, ByteBuffer.allocate(bb.limit()).put(bb.array(), 0, bb.limit()))

            file.seek(head)
            bb.clear()
            var in = file.getChannel()
            var size = in.read(bb)
            while (head + size < tail && size != -1) {
              head += size
              bb.flip();
              bucket.apply(i - 1).write(bb.array(), 0, size)
              bucket.apply(i - 1).flush()
              bb.clear()
              size = in.read(bb)
            }
            if (size != -1) {
              bb.flip();
              bucket.apply(i - 1).write(bb.array(), 0, (tail - head).toInt)
              bucket.apply(i - 1).flush()
            }
            in.close()
            file.close()
            bucket.apply(i - 1).close()
          } catch {
            case e: Exception => {
              e.printStackTrace()
            }
          }
        }
      })
    }
  }

  def startJob(): Unit = {

    val actorSystem = ActorSystem("LocalActorSystem")

    sender = actorSystem.actorOf(Props.create(classOf[MsgSender], actorSystem.actorOf(Props[MsgListener]), omitFile).withRouter(RoundRobinPool(nrOfInstances = dist)))

    doDepart()

    while (threadPool.getActiveCount != 0) {
      Thread.sleep(3000)
    }
    threadPool.shutdown()

    sender ! Get()
    actorSystem.terminate()
  }
}

sealed trait ContentTrait

case class Put(key: Int, bb: ByteBuffer) extends ContentTrait

case class Get() extends ContentTrait

case class Result(value: String) extends ContentTrait

class MsgListener extends Actor {
  private var msgSeq: Map[Int, ByteBuffer] = null

  def MsgListener() {}

  override def preStart() {
    msgSeq = Map()
  }

  override def receive: Actor.Receive = {
    case msg: String => {
      println("listener received: " + msg)
    }
    case Put(key: Int, bb: ByteBuffer) => {
      msgSeq += key -> bb
    }
    case Get() => {
      var total: ByteBuffer = ByteBuffer.allocate(10000);
      msgSeq.toList.sorted foreach {
        v => {
          v._2.flip()
          total.put(v._2.array(), 0, v._2.remaining())
        }
      }
      total.flip()
      sender() ! Result(new String(total.array(), 0, total.remaining()));
      total = null;
    }
  }
}

class MsgSender extends Actor {
  private var msgListener: ActorRef = null
  private var omitFile: File = null

  def this(msgListener: ActorRef, omitFile: File) {
    this()
    this.msgListener = msgListener;
    this.omitFile = omitFile;
  }

  override def receive: Actor.Receive = {
    case msg: String => {
      println("sender received: " + msg)
    }
    case Put(key: Int, bb: ByteBuffer) => {
      msgListener ! Put(key, bb)
    }
    case Get() => {
      msgListener ! Get()
    }
    case Result(value: String) => {
      val omitFos = new FileOutputStream(omitFile)
      omitFos.write(value.getBytes("utf-8"))
      omitFos.flush()
      omitFos.close()
    }
  }
}

object FileDeparter {

  def createDataFile(path: String): Unit = {
    val random = new Random(System.nanoTime());
    var bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path, true)))
    for (i <- 1 to 10000) {
      bw.write(random.nextInt() + "," + random.nextInt() + "," + random.nextInt() + "," + random.nextInt() + "\n")
    }
    bw.flush()
    bw.close()
  }

  def main(args: Array[String]) {

    val start = System.currentTimeMillis();

    createDataFile("./bigdata/user.csv");

    new FileDeparter()("./bigdata/user.csv", 20).startJob();

    //    actorSystem.terminate()

    println(System.currentTimeMillis() - start);
  }
}