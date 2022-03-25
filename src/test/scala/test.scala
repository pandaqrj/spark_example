import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object test {
    def main(args: Array[String]): Unit = {
        var a = mutable.Seq(1)
        val b = a.clone()
        a(0) = a.head+1
        println(a,b)
    }

}

