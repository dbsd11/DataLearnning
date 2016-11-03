package datawhere

import java.io.{BufferedReader, OutputStream, RandomAccessFile}
import java.nio._
import java.util.concurrent.{ArrayBlockingQueue, ThreadPoolExecutor, TimeUnit}

import scala.collection.mutable.ListBuffer
import scala.io.Codec
import scala.reflect.io.File
import scala.util.Random
import scala.util.control.Breaks

/**
  * Created by BSONG on 2016/10/13.
  */
object BigDataRankTest {
   val home = "./bigdata"
   val out = home+"/out"
   val dataFile = File(home + "/BigData")
  private val threadPool:ThreadPoolExecutor = new ThreadPoolExecutor(4,100,1,TimeUnit.SECONDS, new ArrayBlockingQueue[Runnable](100));
  private var heap: Node = null
  private var omit = "";

  def main(args: Array[String]): Unit = {
    createDataFile()
    val start = System.currentTimeMillis()
        val dist = 20;
        departFile(dist)
       while(threadPool.getActiveCount!=0){
            Thread.sleep(1000)
       }
        buildHeap(dist)
        mergeSort()
    println(System.currentTimeMillis()-start)
  }

  /**
    * Step1
    */
  def createDataFile(): Unit = {
    dataFile.deleteRecursively()
    dataFile.createFile();
    val random = new Random(System.nanoTime());
    val writer = dataFile.writer(true, Codec.apply("utf-8"));
    for (i <- 1 to 100000) {
      writer.write(String.valueOf(random.nextInt()) + "\n")
    }
    writer.flush()
    writer.close()
  }

  /**
    * Step2
    */
  def departFile(dist:Int): Unit = {
    var bucket = new Array[OutputStream](dist);
    for (i <- 1 to dist) {
      var file = File(out +"/"+ i + ".out")
      file.deleteRecursively()
      file.createFile()
      bucket.update(i - 1, file.outputStream(true))
    }

    var offset = ((dataFile.length+dist-1)/dist).toInt
    for(i<-1 to dist){
      threadPool.execute(new Runnable {
        override def run(): Unit = {
          try{
              var count =0;
              var file:RandomAccessFile = new RandomAccessFile(dataFile.path,"rw");
              file.skipBytes((i-1)*offset)
              var in = file.getChannel()
              var bb = ByteBuffer.allocate(1024)
              var size=in.read(bb)
              while(count+size<offset && size!= -1){
                count=count+size
                bb.flip();
                bucket.apply(i-1).write(bb.array(),0,size)
                bucket.apply(i-1).flush()
                bb.clear()
                size=in.read(bb)
              }
              if(size!= -1){
                bb.flip();
                bucket.apply(i-1).write(bb.array(),0,offset-count)
                bucket.apply(i-1).flush()
              }
              in.close()
              file.close()
              bucket.apply(i-1).close()
          }catch{
            case e:Exception=>{e.printStackTrace()}
          }
        }
      })
    }

    for(i<-1 to dist){
        threadPool.execute(new Runnable {
          override def run(): Unit = {
              var norder = File(out +"/"+ i + ".out")
              val reader = new BufferedReader(norder.reader(Codec.apply("utf-8")));
              var list = List.newBuilder[Int]
              var str: String = reader.readLine()
              while (str != null) {
                if(str.length>0 && !str.equals("-")){
                  list.+=(Integer.valueOf(str))
                }
                str = reader.readLine()
              }
              reader.close()
              var array = list.result().toArray[Int]
              list = null
              array = array.sorted
              norder.deleteRecursively()
              norder.createFile()
              val writer = norder.writer(true, Codec.apply("utf-8"))
              for (i <- 1 to array.length) {
                writer.write(String.valueOf(array.apply(i - 1)) + "\n")
              }
              writer.close()
              array = null
          }
        })
    }

  }

  /**
    * Step3
    */
  def buildHeap(dist:Int): Unit = {
    heap = new Node()
    var list = new ListBuffer[Node]
    list.+=(heap)
    var leafCount = 1
    var i = 0;
    var lor = 0;
    while (leafCount != dist) {
      val node = new Node()
      list.+=(node)
      if (lor == 0) {
        list.apply(i).left = node
      } else {
        list.apply(i).right = node
        i = i + 1
        leafCount = leafCount + 1
      }
      lor = 1 - lor
    }
    list = null
  }

  /**
    * Step4
    */
  def mergeSort(): Unit = {
    val listBuffer = new ListBuffer[Node]
    heap.serachLeaf(listBuffer)
    val list = listBuffer.result()
    for (i <- 1 to list.size) {
      list.apply(i - 1).source = new BufferedReader(File(out + "/" + i + ".out").reader(Codec.apply("utf-8")))
    }

    val resultWriter = File(out + "/Rank.out-"+System.currentTimeMillis()).createFile().writer(true, Codec.apply("utf-8"))
    Breaks.breakable({
      while (true) {
        heap.elect()
        if (heap.value == null) {
          Breaks.break()
        }
        resultWriter.write(heap.value + "\r\n")
        resultWriter.flush()
        heap.value = null
      }
      resultWriter.close()
    })

    for (i <- 1 to list.size) {
      list.apply(i-1).source.close()
    }
  }

  private class Node {
    var left: Node = null;
    var right: Node = null
    var value: Integer = null;
    var source: BufferedReader = null;

    @SuppressWarnings(Array("Consider forkjoin frame"))
    def serachLeaf(list: ListBuffer[Node]): Unit = {
      if (left == null && right == null) {
        list += this
      } else {
        if (left != null) {
          left.serachLeaf(list)
        }
        if (right != null) {
          right.serachLeaf(list)
        }
      }
    }

    def elect(): Unit = {
      if (value != null) {
        return
      }
      if (left == null && right == null) {
        try {
          value = Integer.parseInt(source.readLine())
        } catch {
          case e: Exception => {}
        }
        return
      }
      if (left != null && left.value == null) {
        left.elect()
      }
      if (right != null && right.value == null) {
        right.elect()
      }
      if (left.value == null) {
        value = right.value
        right.value = null
      }
      if (right.value == null) {
        value = left.value
        left.value = null
      }
      if (left.value != null && right.value != null) {
        if (left.value < right.value) {
          value = left.value
          left.value = null
        } else {
          value = right.value
          right.value = null
        }
      }
    }
  }

}
