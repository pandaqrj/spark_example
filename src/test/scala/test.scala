import org.apache.spark.{SparkConf, SparkContext}

object test {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val rdd1 = sc.makeRDD(List(
            ("a", 1), ("b", 2)//, ("c", 3)
        ))

        val rdd2 = sc.makeRDD(List(
            ("a", 4), ("b", 5),("c", 6),("c", 7)
        ))

        val rdd3 = rdd1.groupByKey()
        val rdd4 = rdd2.groupByKey()
        rdd3.union(rdd4).groupByKey().collect().foreach(println)
    }

}

