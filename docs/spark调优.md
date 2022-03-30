# spark 调优
## 1.持久化
缓存占用的是Storage内存，可在Spark UI上查看。带_2的存储方式是复制一份防止报错。
单独使用缓存会使得数据存储空间很大或会出现数据丢失的情况推荐与 **checkpoint()** 和序列化一起使用。
```scala
/**
 * cache()等于调用persist(MEMORY_ONLY)
 * 占用 Storage内存12G左右
 * 最终报错OOM: Caused by: java.lang.OutOfMemoryError: Java heap spac
 * 大的数据集不推荐使用此方法！
 */
rdd.cache()

/**
 * 默认使用MEMORY_AND_DISK
 * 序列化方法默认使用Encoder序列化
 * 实际试验，SER级别比默认级别占用的存储大
 * 因此编写spark程序优先应该使用SparkSQL，SparkSQL默认的优化已经非常牛逼了
 */
df.cache()
df.persist(StorageLevel.MEMORY_AND_DISK_SER)
```
## 2.序列化
使用 **checkpoint()** 和序列化一起缓存。
```scala
package org.example.rdd.persist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_RDD_KryoSER {
    case class order(
                        userId: String,
                        salesNum: Double
                    )

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf()
            .setMaster("local[*]").setAppName("Persist")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .registerKryoClasses(Array(classOf[order]))
        val sc = new SparkContext(sparkConf)
        sc.setCheckpointDir("cp")

        val rdd = sc.textFile("data/data.txt")
            .map(_.split(", "))
            .map(x => order(x(0), x(1).toDouble))

        /**
         * 消耗Storage内存 2G左右。成功执行！
         */
        rdd.persist(StorageLevel.MEMORY_ONLY_SER_2)
        rdd.checkpoint()
        rdd.foreachPartition(( p: Iterator[order] ) => p.foreach(item => println(item.userId)))
        while(true){}
    }
}
```
## 3.并行度
所谓并行度就是Task的数量，也可以说是分区的数量。

RDD默认并行度：**spark.default.parallelism**，默认是没有设置，是按照Task的数目设置的。

Task被执行的并发度 = Executor数目 * 每个Executor核数。

SparkSQL默认并发度：**spark.sql.shuffle.partitions** 默认为200。

注意：RDD与SparkSQL的并发度设置相互独立。

#### CPU低效的原因：
1. 并行度过低，数据分片较大导致CPU线程挂起。
2. 并行度过高，数据过于分散让调度开销更多。

#### 合理的并行度：
集群处理器核数的2-3倍。

## 4.数据倾斜
#### 现象：
可以在Spark UI看到task执行时间的条形图，如果有某些task的条形图特别长，就是发生了数据倾斜！就是这些任务可能会报出OOM异常。

数据倾斜只会发生在shuffle过程中。
这里给大家罗列一些常用的并且可能会触发shuffle操作的算子：**distinct、groupByKey、reduceByKey、aggregateByKey、join、cogroup、repartition**等。
出现数据倾斜时，可能就是你的代码中使用了这些算子中的某一个所导致的。

spark.sql.adaptive.skewJoin.enabled true

spark.sql.adaptive.skewJoin.skewedPartitionFactor 5

spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes 256MB
### 4.1 sample抽样定位倾斜的KEY
**sample()** 算子抽样。
```scala
val df = spark.read.parquet("spark-warehouse/user_sales_order_detail")
val dfSample = df.select("user_id")
    .sample(withReplacement = false, 0.01)
    .groupBy("user_id")
    .count()
    .orderBy(col("count").desc)
dfSample.show()
```
### 4.2 单表倾斜
map端预聚合，或者随机打散KEY。
### 4.3 广播JOIN处理
对小表mapjoin
### 4.4 随机拆分KEY
对大表随机数打散KEY，对小表进行等量扩容。
## 5.Map端优化
### 5.1 Map端预聚合
SparkSQL会自动优化，RDD优先使用reduceByKey aggregateByKey foldByKey等算子
### 5.2 小文件读取
spark.files.openCostInBytes 默认4M

spark.sql.files.openCostInBytes 默认4M

以上设置仅对Parquet，JSON 以及 ORC 格式文件有效
### 5.3 调整ShuffleWrite的溢写输出流缓冲区大小
spark.shuffle.file.buffer 默认32K，可以加大，但是要注意有可能会造成OOM

## 6.Reduce端优化
### 6.1 小文件输出优化-并行度与分区
重分区：repartition() coalesce()等算子。
### 6.2 动态分区小文件优化
SparkSQL3.2可以自动动态分区调整
### 6.3 调整Reduce缓冲区&重做次数&重试等待间隔

### 6.4 合理使用bypass

## 7. JOB优化
### 7.1 调节数据本地化等待时间

### 7.2 堆外内存

### 7.3 使用堆外缓存

### 7.4 调整链接等待市场

## 8.SparkSQL语法优化
### 8.1 谓词下推
将过滤条件的谓词逻辑尽可能提前执行，减少下游处理的数据量。能够大幅度减少数据扫描两，降低磁盘I/O开销。

普通代码：
```roomsql
    SELECT T1.ID
      FROM T1
 LEFT JOIN T2
        ON T1.ID = T2.ID
     WHERE T1.TYPE = 'A'
       AND T2.SALES_NUM >= '1'
```
谓词下推后的代码：
```roomsql
    SELECT T1.ID
      FROM(
        SELECT T1.ID
          FROM T1
         WHERE T1.TYPE = 'A'
      ) T1
 LEFT JOIN(
        SELECT T2.ID
          FROM T2
         WHERE T2.SALES_NUM >= '1'
      ) T2
        ON T1.ID = T2.ID
```
### 8.2. 广播JOIN

```scala
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
```
### 8.3. SMB JOIN
Sort Merge Bucket Join

两个分桶表使用分桶KEY作JOIN，需要分桶数量和规则相等！
```roomsql
CREATE TABLE IF NOT EXISTS BUCK1(
    user_id String,
    sales_num Double
)
CLUSTERED BY user_id SORTED BY user_id into 100 buckets;
CREATE TABLE IF NOT EXISTS BUCK2(
    user_id String,
    user_name String
)
CLUSTERED BY user_id SORTED BY user_id into 100 buckets;
```
然后直接JOIN。对于不是分桶表的数据，可以先做转换:
```scala
sparkSession.read.json("xxx")
    .write.partitionBy("dt", "dn")
    .format("parquet")
    .bucketBy(5, "orderid")
    .sortBy("orderid")
    .mode(SaveMode.Overwrite)
    .saveAsTable("BUCK1")

sparkSession.read.json("xxx")
    .write.partitionBy("dt", "dn")
    .bucketBy(5, "orderid")
    .format("parquet")
    .sortBy("orderid")
    .mode(SaveMode.Overwrite)
    .saveAsTable("sBUCK2")
```
### 8.4 Hint

```scala
// TODO 广播Join的hint
sparkSession.sql("select /*+ BROADCAST(school) */ *  FROM t;
sparkSession.sql("select /*+ BROADCASTJOIN(school) */ *  FROM t;
sparkSession.sql("select /*+ MAPJOIN(school) */ *  FROM t;

// TODO SortMergeJoin的hint
sparkSession.sql("select /*+ SHUFFLE_MERGE(school) */ * FROM t")
sparkSession.sql("select /*+ MERGEJOIN(school) */ * FROM t")
sparkSession.sql("select /*+ MERGE(school) */ * FROM t")

// TODO shuffle hash join 的hint
sparkSession.sql("select /*+ SHUFFLE_HASH(school) */ * FROM t")

// TODO SHUFFLE_REPLICATE_NL join 的hint
sparkSession.sql("select /*+ SHUFFLE_REPLICATE_NL(school) */ * FROM t")

// TODO 重分区hints
sparkSession.sql("SELECT /*+ COALESCE(3) */ * FROM t")
sparkSession.sql("SELECT /*+ REPARTITION(3) */ * FROM t")
sparkSession.sql("SELECT /*+ REPARTITION(c) */ * FROM t")
sparkSession.sql("SELECT /*+ REPARTITION(3, c) */ * FROM t")
sparkSession.sql("SELECT /*+ REPARTITION_BY_RANGE(c) */ * FROM t")
sparkSession.sql("SELECT /*+ REPARTITION_BY_RANGE(3, c) */ * FROM t")
sparkSession.sql("SELECT /*+ REBALANCE */ * FROM t")
sparkSession.sql("SELECT /*+ REBALANCE(c) */ * FROM t")
```