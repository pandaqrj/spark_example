package org.example.optimization

import org.apache.spark.sql.SparkSession

import java.io.File

object sparkOpt02_DataSkew {
    def main(args: Array[String]): Unit = {

        //TODO - 数据倾斜
        val spark = SparkSession.builder()
            .master("local[*]")
            .getOrCreate()
        spark.read.parquet("spark-warehouse/user_sales_order_detail").createOrReplaceTempView("user_sales_order_detail")
        spark.sql(
            """
              |     select user_id
              |           ,sum(sales_num)
              |       from user_sales_order_detail t1
              |   group by user_id
              |""".stripMargin).show()
        while(true){}
    }
}
