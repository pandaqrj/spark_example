package org.example.streaming

import java.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming02_Receiver {

    def main(args: Array[String]): Unit = {

        // TODO - 自定义接收器
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val messageDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver())
        messageDS.print()

        ssc.start()
        ssc.awaitTermination()
    }
    /**
    自定义数据采集器
    1. 继承Receiver，定义泛型, 传递参数
    2. 重写方法
    **/
    class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
        private var flg = true // 标记，表示采集器是否继续执行
        override def onStart(): Unit = {
            new Thread(new Runnable { // 生成一个新线程去采集数据
                override def run(): Unit = {
                    while ( flg ) {
                        val message = "采集的数据为：" + new Random().nextInt(10).toString
                        store(message) // 存储生成的数据，存储的级别就是StorageLevel的级别
                        Thread.sleep(500)
                    }
                }
            }).start()
        }

        override def onStop(): Unit = {
            flg = false // 将flg置否，停止采集器
        }
    }
}
