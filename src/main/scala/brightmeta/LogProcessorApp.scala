package brightmeta

import java.util.Properties

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import scala.collection.mutable.{Map => MMap}

/**
  * Created by John on 6/6/17.
  */

case class Visits(hostId: String, visitorIP: String)
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
      new Visits(keyVal(0), keyVal(1)) // (pageId, IP)
    })

    val keyedMessageStream = messageStream.keyBy(_.hostId)
      .mapWithState({
        (log, requesters:Option[(Int, MMap[String, Int])]) => {
          var requester = requesters.getOrElse((0, MMap[String, Int]().withDefaultValue(0)))
          val count = requester._1 + 1
          requester._2.update(log.visitorIP, requester._2(log.visitorIP) + 1)
          (log.hostId, Some((requester)))
        }
      })

    val sinkFunction = new FlinkKafkaProducer010[String]("notifications",new SimpleStringSchema(), properties)

    messageStream.map(log => log.visitorIP + "," + log.hostId)
      .addSink(sinkFunction)

    env.execute()
  }
}