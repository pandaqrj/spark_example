package org.example.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark04_countByKey {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        //val rdd = sc.makeRDD(List(1,1,1,4),2)
        val rdd = sc.makeRDD(List(
            ("a", 1),("a", 2),("a", 3),("b",6)
        ))

        // TODO - countByKey

       //val intToLong: collection.Map[Int, Long] = rdd.countByValue()
        //println(intToLong)
        val stringToLong: collection.Map[String, Long] = rdd.countByKey()
        println(stringToLong)

        sc.stop()

    }
}
