package org.example.acc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {
    def main(args: Array[String]): Unit = {

        // TODO - 核心数据结构只写变量累加器

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
        val sc = new SparkContext(sparkConf)
        val rdd = sc.parallelize(Seq(1,2,3,4))

        // 错误演示：driver的变量传递到rdd的分区中计算，但是driver无法得到分区中的返回值
        var sum  = 0
        rdd.foreach(
            x => {
                sum += sum + x
            }
        )
        println(sum)

        /**
         * 累加器
         * spark默认提供了 long double collection 三种累加器
         */
        val sumAcc = sc.longAccumulator("sum")
        rdd.foreach(
            x => {
                //使用累加器
                sumAcc.add(x)
            }
        )
        println(sumAcc.value)

        /**
         * 少加：
         * 如果 rdd 没有触发 action 算子，transform算子是不会执行的，因此如果在 transform 算子中编写了累加器，需要记得触发 action
         */
        val sumAcc1 = sc.longAccumulator("sum1")
        rdd.map(
            x => {
                //使用累加器
                sumAcc1.add(x)
                x
            }
        )
        println(sumAcc1.value)

        /**
         * 多加：
         * 如果触发了行动算子，但是如果触发多次，RDD实际逻辑会执行多次！因此需要十分的注意！！！
         * 因此 累加器 一般卸载 action 算子中
         */
        val sumAcc2 = sc.longAccumulator("sum2")
        val rdd2 = rdd.map(
            x => {
                //使用累加器
                sumAcc2.add(x)
                x
            }
        )
        rdd2.collect()
        rdd2.collect()
        println(sumAcc2.value)

        sc.stop()
    }
}
