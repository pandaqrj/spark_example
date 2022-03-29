package org.example.optimization

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object spark03_Opt_BroadCastJoin {
    def main(args: Array[String]): Unit = {

        // TODO - BroadCastJoin 并查看执行计划

        val spark = SparkSession.builder().master("local[*]").appName("BroadCastJoin").config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate()
        import spark.implicits._
        val df1 = Seq(
            (1,2,3),
            (1,2,3)
        ).toDF("c1", "c2", "c3")
        val df2 = Seq(
            (1,4,5),
        ).toDF("c1", "c2", "c3")
        df1.createOrReplaceTempView("df1")
        df2.createOrReplaceTempView("df2")

        /**
         * 外连接只能对子表进行广播，无法广播主表
         * 内连接都可以！
         * spark.sql.autoBroadcastJoinThreshold 参数可以设置广播数据集的大小
         * hints MAPJOIN BROADCAST BROADCASTJOIN 都是一样的
         * 使用DATAFRAME API broadcast广播数据集
         * df3 和 df4 是一样的
         */
        val df3 = spark.sql(
            """
              |select /*+ MAPJOIN(t1) */ t1.c1, sum(t2.c2), sum(t2.c3)
              |from df2 t1
              |join df1 t2
              |on t1.c1 = t2.c1
              |group by  t1.c1
              |""".stripMargin)

        val df4  = broadcast(df2).join(df1, Seq("c1"))
            .select(df2("c1"), df1("c2"), df1("c3"))
            .groupBy(df2("c1"))
            .agg(sum(df1("c2")), sum(df1("c3")))

        df3.show()
        df4.show()

        df3.explain()
        df4.explain()
        //while(true){} //用来防止进程结束，来查看spark UI
    }
}
