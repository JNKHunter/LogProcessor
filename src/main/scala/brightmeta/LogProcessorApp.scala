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
      (keyVal(0), keyVal(1))
    }).keyBy(_._1)
      .mapWithState({
        (log, requesters:Option[MMap[String, Int]]) => {
          val requester = requesters.getOrElse(MMap[String, Int]().withDefaultValue(0))
          requester.update(log._2, requester(log._2) + 1)
          (log, Some(requester))
        }
      })

    val sinkFunction = new FlinkKafkaProducer010[String]("notifications",new SimpleStringSchema(), properties)

    messageStream.map(log => log._1 + "," + log._2)
      .addSink(sinkFunction)

    env.execute()
  }
}