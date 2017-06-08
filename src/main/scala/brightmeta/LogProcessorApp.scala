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

class HostGroup {
  var hostId:String = _
  var requestCount = 0
  var requestMap = MMap[String, Int]()
  
  def addRequest = {requestCount += 1}

}

case class Notification(hostId:String, ddos:Boolean)

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

    /*val keyedMessageStream: DataStream[(String, (Int, MMap[String,Int]))] = messageStream.keyBy(_.hostId)
      .mapWithState({
        (log: Visit, requesters:Option[(Int, MMap[String, Int])]) => {
          val requester = requesters.getOrElse((0, MMap[String, Int]().withDefaultValue(0)))
          requester._2.update(log.visitorIP, requester._2(log.visitorIP) + 1)

          (log, Some((requester._1 + 1, requester._2)))
        }
      })*/

    val requestThreshold = 10

    val windowStream = messageStream.keyBy(_.hostId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .fold(new HostGroup()){
        (group, visit) => {
          group.addRequest
          group.hostId = visit.hostId
          var requesterTotal = group.requestMap.getOrElse(visit.visitorIP, 0)
          requesterTotal += 1
          group.requestMap.update(visit.visitorIP, requesterTotal)
          group
        }
      }.map(group => {
      var reqPerWindow = group.requestCount/group.requestMap.size

      var isDDos = false
      if(group.requestCount/group.requestMap.size > requestThreshold) isDDos = true
      new Notification(group.hostId, isDDos)
      
    }).filter(notification => (
      notification.ddos
    ))

    val sinkFunction = new FlinkKafkaProducer010[String]("notifications",new SimpleStringSchema(), properties)


    windowStream.map(notification => notification.hostId).addSink(sinkFunction)

    env.execute()
  }
}