package org.example.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount {

    /**
     * 最基本的sparkStreaming程序，会统计一秒内输入的单词数量并打印
     * @param args
     */
    def main(args: Array[String]): Unit = {

        // TODO 简单的单词实时统计
        // StreamingContext创建时，需要传递两个参数
        // 第一个参数表示环境配置
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        // 第二个参数表示批量处理的周期（采集周期）
        val ssc = new StreamingContext(sparkConf, Seconds(1))

        // TODO 单词统计，会发现DStream的算子和RDD的几乎一摸一样。
        // 获取端口数据
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        val words = lines.flatMap(_.split(" "))

        val wordToOne = words.map((_,1))

        val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_+_)

        wordToCount.print()

        // 由于SparkStreaming采集器是长期执行的任务，所以不能直接关闭
        // 如果main方法执行完毕，应用程序也会自动结束。所以不能让main执行完毕
        //ssc.stop()
        // 1. 启动采集器 之前的代码都是属于定于数据的计算逻辑，如果没有启动，则计算逻辑不会被执行
        ssc.start()
        // 2. 等待采集器的关闭
        ssc.awaitTermination()
    }
}
