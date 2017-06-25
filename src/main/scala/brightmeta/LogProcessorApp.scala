package brightmeta

import java.io.FileInputStream
import java.util.{Properties, ResourceBundle}

import brightmeta.data.{HostGroup, Log, LogDeserializationSchema}
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool

/**
  * Created by John on 6/6/17.
  */

case class Notification(hostId: String, ddos: Boolean)

object LogProcessorApp {
  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)

    val properties = new Properties()
    val config: ResourceBundle = ResourceBundle.getBundle("config")
    
    properties.setProperty("bootstrap.servers", params.get("bootstrap.servers",
      config.getString("bootstrap.servers")))
    properties.setProperty("zookeeper.connect", params.get("zookeeper.connect",
      config.getString("zookeeper.connect")))
    properties.setProperty("group.id", params.get("group.id",
      config.getString("group.id")))

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(params.getInt("parallelism", config.getString("parallelism").toInt))

    val requestThreshold = config.getString("requestThreshold").toInt

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

    val keyedStream = env.addSource(sourceFunction)
      .keyBy(_.getPartitionKey)

    val windowStream = keyedStream
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .fold(new HostGroup()) {
        (group, visit) => {
          group.setHostId(visit.getHostId)
          group.addIp(visit.getVisitorIP)
          group
        }
      }.map(group => {
      var reqPerWindow = group.getRequestCount / group.getNumberOfRequesters

      if (reqPerWindow > requestThreshold) {
        group.setDDos(true)
      }
      group

    }).filter(group => (
      group.isDDos
      ))

    val foldedWindowStream = windowStream.keyBy(_.getHostId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .reduce((group1, group2) => {

        group1.setRequestCount(group1.getRequestCount + group2.getRequestCount)
        group1
      })

    val sinkFunction = new FlinkKafkaProducer010[String]("notifications", new SimpleStringSchema(), properties)

    foldedWindowStream.map(group => {
      group.getHostId + "," + group.getRequestCount
    }).addSink(sinkFunction)

    env.execute()
  }
}