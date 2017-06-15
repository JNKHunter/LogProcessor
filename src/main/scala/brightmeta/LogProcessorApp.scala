package brightmeta

import java.util.Properties

import brightmeta.data.{Log, LogDeserializationSchema}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool

import scala.collection.mutable.{Map => MMap}

/**
  * Created by John on 6/6/17.
  */

class HostGroup {
  var machineId:String = _
  var hostId:String = _
  var requestCount = 0
  var requestMap = MMap[String, Int]()
  
  def addRequest = {requestCount += 1}

}

case class Notification(hostId:String, ddos:Boolean)

object LogProcessorApp {
  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", params.get("bootstrap.servers", "localhost:9092"))
    properties.setProperty("zookeeper.connect", params.get("zookeeper.connect", "localhost:2181"))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(params.getInt("parallelism", 1))

    val sourceFunction = new FlinkKafkaConsumer010[Log]("logs-replicated-10", new LogDeserializationSchema, properties)
    properties.setProperty("group.id", params.get("group.id", "group1"))

    /*val keyedMessageStream: DataStream[(String, (Int, MMap[String,Int]))] = messageStream.keyBy(_.hostId)
      .mapWithState({
        (log: Visit, requesters:Option[(Int, MMap[String, Int])]) => {
          val requester = requesters.getOrElse((0, MMap[String, Int]().withDefaultValue(0)))
          requester._2.update(log.visitorIP, requester._2(log.visitorIP) + 1)

          (log, Some((requester._1 + 1, requester._2)))
        }
      })*/

    val requestThreshold = 5

    val keyedStream: KeyedStream[Log, (String, String)] = env.addSource(sourceFunction).keyBy(_.getKey)
      .map(log => {
        println(log.getMachineKey)
        log
      }).keyBy(_.getKey)

    val windowedStream = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))

    val foldedStream = windowedStream.fold(new HostGroup()){
      (group, visit) => {
        group.addRequest
        group.hostId = visit.getHostId
        var requesterTotal = group.requestMap.getOrElse(visit.getVisitorIP, 0)
        requesterTotal += 1
        group.requestMap.update(visit.getVisitorIP, requesterTotal)
        group
      }
    }

    val mappedFoldedStream = foldedStream.map(group => {
      var reqPerWindow = group.requestCount/group.requestMap.size

      var isDDos = false
      if(group.requestCount/group.requestMap.size > requestThreshold) isDDos = true
      new Notification(group.hostId, isDDos)

    })

    val filteredStream =  mappedFoldedStream.filter(notification => (
      notification.ddos
      )).keyBy(_.hostId).reduce((notification1, notification2) => notification1)

    val sinkFunction = new FlinkKafkaProducer010[String]("notifications",new SimpleStringSchema(), properties)

    filteredStream.map(notification => notification.hostId).addSink(sinkFunction)

    env.execute()
  }
}