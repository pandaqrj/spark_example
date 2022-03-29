package org.example.optimization

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Opt_Cache {

    case class order(userId: String, salesNum: Double)
    def main(args: Array[String]): Unit = {

        // TODO - 测试普通cache，并查看storage内存占用
        val sparkConf = new SparkConf()
            .setMaster("local[*]").setAppName("Persist")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.textFile("data/data.txt")
            .map(_.split(", "))
            .map(x => order(x(0), x(1).toDouble))
        rdd.persist()
        rdd.foreachPartition((p: Iterator[order]) => p.foreach(item => println(item.userId)))
        while (true) {}
    }
}
