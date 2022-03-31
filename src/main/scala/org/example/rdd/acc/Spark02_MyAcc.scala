package org.example.rdd.acc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark02_MyAcc {
    def main(args: Array[String]): Unit = {

        // TODO - 自定义累加器 wordCount

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
        val sc = new SparkContext(sparkConf)
        val rdd = sc.parallelize(Seq("a", "a", "b", "c"))

        // 定义注册累加器
        val myAcc = new MyAcc()
        sc.register(myAcc, "MyAcc")

        rdd.foreach(
            x => {
                // 使用累加器
                myAcc.add(x)
            }
        )
        println(myAcc.value)

        sc.stop()
    }

    /**
     * 自定义累加器
     * 继承 AccumulatorV2，有两个泛型
     * IN：输入数据
     * OUT：输出数据
     */
    class MyAcc extends AccumulatorV2[String, mutable.Map[String, Long]]{

        // 判读初始状态
        override def isZero: Boolean = {
            this.wordCount.isEmpty
        }

        // 复制
        override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
            val newAcc = new MyAcc()
            newAcc.wordCount = this.wordCount.clone()
            newAcc
        }

        // 重置
        override def reset(): Unit = {
            this.wordCount.clear()
        }

        // 累加的值
        private var wordCount = mutable.Map[String, Long]()

        // 累加方法
        override def add(v: String): Unit = {
            val newCnt = this.wordCount.getOrElse(v, 0L) + 1
            this.wordCount.update(v, newCnt)
        }

        // 累加器合并
        override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
            val otherMap = other.value
            otherMap.foreach{
                case (k, v) => {
                    val newCnt = this.wordCount.getOrElse(k, 0L) + 1
                    this.wordCount.update(k, newCnt)
                }
            }
        }

        // 累加的值
        override def value: mutable.Map[String, Long] = this.wordCount
    }
}
