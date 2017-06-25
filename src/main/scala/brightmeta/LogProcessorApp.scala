package brightmeta

import java.util.Properties

import brightmeta.data.{HostGroup, Log, LogDeserializationSchema}
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool

import scala.collection.mutable.{Map => MMap}

/**
  * Created by John on 6/6/17.
  */

case class Notification(hostId: String, ddos: Boolean)

object LogProcessorApp {
  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", params.get("bootstrap.servers", "localhost:9092"))
    properties.setProperty("zookeeper.connect", params.get("zookeeper.connect", "localhost:2181"))
    properties.setProperty("group.id", params.get("group.id", "group1"))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(params.getInt("parallelism", 10))

    class LogPartitioner extends Partitioner[String] {
      override def partition(key: String, numPartitions: Int): Int = {
        Integer.parseInt(key)
      }
    }

    val sourceFunction = new FlinkKafkaConsumer010[Log]("logs-replicated-10", new LogDeserializationSchema, properties)
    /*val keyedMessageStream: DataStream[(String, (Int, MMap[String,Int]))] = messageStream.keyBy(_.hostId)
      .mapWithState({
        (log: Visit, requesters:Option[(Int, MMap[String, Int])]) => {
          val requester = requesters.getOrElse((0, MMap[String, Int]().withDefaultValue(0)))
          requester._2.update(log.visitorIP, requester._2(log.visitorIP) + 1)

          (log, Some((requester._1 + 1, requester._2)))
        }
      })*/

    val requestThreshold = 100
    val keyedStream = env.addSource(sourceFunction)
      .keyBy(_.getPartitionKey)

    val windowStream = keyedStream
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .fold(new HostGroup()) {
        (group, visit) => {
          group.setHostId(visit.getHostId);
          group.addIp(visit.getVisitorIP);
          group
        }
      }.map(group => {
      var reqPerWindow = group.getRequestCount / group.getNumberOfRequesters

      if (reqPerWindow > requestThreshold) {
        group.setDDos(true);
      }
      group

    }).filter(group => (
      group.isDDos
      ))

    val foldedWindowStream = windowStream.keyBy(_.getHostId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .reduce((group1, group2) => {

        group1.setRequestCount(group1.getRequestCount + group2.getRequestCount);

        //TODO: Replace this garbage with a way to get the top 10 requesters (maybe a priority queue)
        /*val mergedMap = group1.requestMap ++ group2.requestMap.map {
          case (ip,count) => ip -> (count + group1.requestMap.getOrElse(ip,0)) }

        group1.requestMap = mergedMap*/
        group1
      })

    val sinkFunction = new FlinkKafkaProducer010[String]("notifications", new SimpleStringSchema(), properties)

    foldedWindowStream.map(group => {
      group.getHostId + "," + group.getRequestCount
    }).addSink(sinkFunction)

    env.execute()
  }
}