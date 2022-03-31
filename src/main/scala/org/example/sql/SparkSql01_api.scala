package org.example.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSql01_api {

    def main(args: Array[String]): Unit = {

        // TODO - SparkSql API测试
        val spark = SparkSession.builder()
            .config("spark.sql.shuffle.partitions", "36")
            .master("local[*]")
            .getOrCreate()
        val df = spark.read.parquet("spark-warehouse/user_sales_order_detail")
        val df2 = spark.read.parquet("spark-warehouse/user_info").distinct().toDF()

        // TODO - 1.API写一个简单的关联聚合并查看执行计划
        df.join(df2, df("user_id") === df2("user_id"), "left")
            .groupBy(df2("user_id"))
            .agg(count(df2("user_id")).as("cnt"))
            .explain()

        // TODO - 2.直接用SQL写，并查看执行计划
        df.createOrReplaceTempView("user_sales_order_detail")
        df2.createOrReplaceTempView("user_info")
        spark.sql(
            """
              |     select t2.user_id, count(t2.user_id) as cnt
              |       from user_sales_order_detail t1
              |  left join user_info t2
              |         on t1.user_id = t2.user_id
              |   group by t2.user_id
              |""".stripMargin).explain()

        // TODO - 3.结论：两个的执行计划一样。都会经过 谓词下推、预聚合、MAPJOIN 等自动优化

    }
}
