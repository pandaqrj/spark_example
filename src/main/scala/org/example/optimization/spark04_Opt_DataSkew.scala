package org.example.optimization

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random


object spark04_Opt_DataSkew {

    case class NewOrder(userId: String, salesNum: Double, newUserId: String)
    case class NewUser(userId: String, newUserId: String)
    def main(args: Array[String]): Unit = {

        // TODO - 数据倾斜
        val spark = SparkSession.builder()
            .config("spark.sql.shuffle.partitions", "36")
            .master("local[*]")
            .getOrCreate()
        val df = spark.read.parquet("spark-warehouse/user_sales_order_detail")
        val df2 = spark.read.parquet("spark-warehouse/user_info").distinct().toDF()


//        // TODO - 1.抽样获取倾斜KEY
//        df.cache()
//        val dfSample = df.select("user_id")
//            .sample(withReplacement = false, 0.01)
//            .groupBy("user_id")
//            .count()
//            .orderBy(col("count").desc)
//            .limit(100)
//        dfSample.show()
//
//        // TODO - 2.打散倾斜的KEY
//        import spark.implicits._
//        val newOrder = df.mapPartitions(iter => {
//            iter.map(x => {
//                val userId = x.getAs[String]("user_id")
//                val salesNum = x.getAs[Double]("sales_num")
//                val randInt = Random.nextInt(36)
//                NewOrder(userId,
//                    salesNum,
//                    randInt + "_" + userId)
//            })
//        }).toDF()
//
//        // TODO - 3.小表扩容
//        val newUser = df2.flatMap(x => {
//            val list = ArrayBuffer[NewUser]()
//            val userId = x.getAs[String]("user_id")
//            for(i <- 0 to 35){
//                list.append(NewUser(userId, i + "_" + userId))
//            }
//            list
//        }).toDF()
//
//        // TODO - 4.倾斜的大KEY与扩容后的表进行join
//        newOrder.createOrReplaceTempView("newOrder")
//        newUser.createOrReplaceTempView("newUser")
//        spark.sql(
//            """
//              |    select t2.UserId
//              |          ,sum(t1.salesNum)
//              |      from newOrder t1
//              | left join newUser t2
//              |        on t1.newUserId = t2.newUserId
//              |  group by t2.UserId
//              |""".stripMargin).show()

        // TODO - 5.交给Spark自己处理，会直接使用预聚合和MAPJOIN
        df.createOrReplaceTempView("user_sales_order_detail")
        df2.createOrReplaceTempView("user_info")
        spark.sql(
            """
              |     select t2.user_id, sum(t1.sales_num) as sales_num
              |       from user_sales_order_detail t1
              |  left join user_info t2
              |         on t1.user_id = t2.user_id
              |   group by t2.user_id
              |""".stripMargin).write.mode("overwrite").save("spark-warehouse/dws_user_sales_detail")

        // TODO - 6.结论：以寻常的场景来看，使用map端聚合和mapjoin来解决数据倾斜的问题就可以了。所谓的二次聚合在一般场景的效果来看不如spark自己优化的效果。

        // while(true){}

    }
}
