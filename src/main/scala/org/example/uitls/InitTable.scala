package org.example.uitls

import org.apache.spark.sql.SparkSession

object InitTable {

    def main(args: Array[String]): Unit = {

        // TODO - 创建sparkSQL表
        val spark = SparkSession.builder().master("local[*]").getOrCreate()
        val df1 = spark.read.textFile("data/data.txt")
            .selectExpr("split(value, ', ')[0] as user_id", "cast(split(value, ', ')[1] as double) as sales_num")
        df1.repartition().write.mode("overwrite").save("spark-warehouse/user_sales_order_detail")

        val df2 = spark.read.textFile("data/user.txt")
            .selectExpr("value as user_id")
        df2.write.mode("overwrite").save("spark-warehouse/user_info")
    }
}
