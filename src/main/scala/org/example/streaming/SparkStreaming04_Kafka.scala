package org.example.streaming



import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf

import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object SparkStreaming04_Kafka {

    def main(args: Array[String]): Unit = {

        // TODO - 订阅和消费kafka 需要本地启动kafka并创建 topic 和生产者消费者，详细请自行学习kafka

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "example",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )
        val topics = Array("topicA", "topicB")

        val stream = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )
        stream.map(record => (record.key, record.value)) // value就是topic里面的值


        ssc.start()
        ssc.awaitTermination()
    }

}
