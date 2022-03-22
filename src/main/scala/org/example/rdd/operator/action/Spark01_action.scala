package org.example.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_action {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4))

        // TODO - action
        // 所谓的行动算子，其实就是触发作业(Job)执行的方法
        // 底层代码调用的是环境对象的runJob方法
        // 底层代码中会创建ActiveJob，并提交执行。

        // reduce
        val i: Int = rdd.reduce(_+_)
        println(i)

        // collect : 方法会将不同分区的数据按照分区顺序采集到Driver端内存中，形成数组
        val ints1 = rdd.collect()
        println(ints1.mkString(","))

        // count : 数据源中数据的个数
        val cnt = rdd.count()
        println(cnt)

        // first : 获取数据源中数据的第一个
        val first = rdd.first()
        println(first)

        // take : 获取N个数据
        val ints2: Array[Int] = rdd.take(3)
        println(ints2.mkString(","))

        // takeOrdered : 数据排序后，取N个数据
        val rdd1 = sc.makeRDD(List(4,2,3,1))
        val ints3: Array[Int] = rdd1.takeOrdered(3)
        println(ints3.mkString(","))

        sc.stop()

    }
}
