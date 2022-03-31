package org.example.optimization

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Opt_KryoSER {
    case class order(userId: String, salesNum: Double)

    def main(args: Array[String]): Unit = {

        // TODO - 序列化缓存
        val sparkConf = new SparkConf()
            .setMaster("local[*]").setAppName("Persist")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(Array(classOf[order]))
        val sc = new SparkContext(sparkConf)
        sc.setCheckpointDir("cp")

        val rdd = sc.textFile("data/data.txt")
            .map(_.split(", "))
            .map(x => order(x(0), x(1).toDouble))

        rdd.persist(StorageLevel.MEMORY_ONLY_SER)
        rdd.checkpoint()
        rdd.foreachPartition((p: Iterator[order]) => p.foreach(item => println(item.userId)))
        while (true) {}
    }
}
