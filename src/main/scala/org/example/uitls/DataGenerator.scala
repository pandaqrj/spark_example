package org.example.uitls

import java.io._
import scala.util.Random


object DataGenerator {
    def main(args: Array[String]): Unit = {

        // TODO - 创建测试数据
        val writeFile = new File("data/data.txt")
        val writer = new BufferedWriter(new FileWriter(writeFile))
        for(i <- 1 to 100000000){
            writer.write(s"${Random.nextInt(1000000)}, ${(Random.nextDouble() * Random.nextInt(100)).formatted("%.2f")}\n")
            if( i % 1000000 == 0) println(i)
        }
        for(i <- 1 to 10000000){
            writer.write(s"101, ${(Random.nextDouble() * Random.nextInt(100)).formatted("%.2f")}\n")
            writer.write(s"102, ${(Random.nextDouble() * Random.nextInt(100)).formatted("%.2f")}\n")
            if( i % 1000000 == 0) println(i)
        }
        for(i <- 1 to 50000000){
            writer.write(s"104, ${(Random.nextDouble() * Random.nextInt(100)).formatted("%.2f")}\n")
            if( i % 1000000 == 0) println(i)
        }
        writer.close()

        val writeFile1 = new File("data/user.txt")
        val writer1 = new BufferedWriter(new FileWriter(writeFile1))
        for(i <- 1 to 1000000){
            writer1.write(s"${Random.nextInt(1000000)}\n")
            if( i % 1000000 == 0) println(i)
        }
        writer1.close()
    }
}
