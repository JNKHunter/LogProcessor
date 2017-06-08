package brightmeta

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import scala.collection.mutable.{Map => MMap}

/**
  * Created by John on 6/6/17.
  */

case class Visit(hostId: String, visitorIP: String)

class WindowSnapshot {
  private var _requestCount: Int = 0
  private var ddos = false
  private var _requestStructure: MMap[String, MMap[String, Int]] = MMap[String, MMap[String, Int]]()

  def requestCount = _requestCount
  def requestStructure = _requestStructure

  def addRequest = {_requestCount += 1}

}
//object Visits

object LogProcessorApp {
  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "group1")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceFunction = new FlinkKafkaConsumer010[String]("logs", new SimpleStringSchema(), properties)

    val messageStream = env.addSource(sourceFunction).map(string => {
      val keyVal = string.split(",")
      new Visit(keyVal(0), keyVal(1)) // (hostId, visitorIp)
    })

    //TODO remove this test counter and debug logic

    /*val keyedMessageStream: DataStream[(String, (Int, MMap[String,Int]))] = messageStream.keyBy(_.hostId)
      .mapWithState({
        (log: Visit, requesters:Option[(Int, MMap[String, Int])]) => {
          testCounter += 1
          val requester = requesters.getOrElse((0, MMap[String, Int]().withDefaultValue(0)))
          requester._2.update(log.visitorIP, requester._2(log.visitorIP) + 1)

          (log, Some((requester._1 + 1, requester._2)))
        }
      })*/


    val windowStream = messageStream.keyBy(_.hostId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .fold(new WindowSnapshot()){
        (acc, visit) => {

          val hostAcc = acc.requestStructure.getOrElse(visit.hostId, MMap[String, Int]())
          var requestAcc = hostAcc.getOrElse(visit.visitorIP, 0)
          requestAcc += 1

          hostAcc.update(visit.visitorIP, requestAcc)
          acc.addRequest
          //Determine when we should push to the notification stream
          acc
        }
      }

    val sinkFunction = new FlinkKafkaProducer010[String]("notifications",new SimpleStringSchema(), properties)


    windowStream.map(acc => acc.requestCount.toString)
      .addSink(sinkFunction)
    
    /*messageStream.map(log => log.hostId + "," + log.visitorIP)
      .addSink(sinkFunction)*/

    env.execute()
  }
}