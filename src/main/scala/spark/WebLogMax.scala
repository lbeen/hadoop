package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object WebLogMax {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("WebLogByHost").setMaster("local[*]"))
    //读取符合要求的行
    val lines = sc.textFile("/spark_test/weblog2.txt").map(_.split("\t"))

    val partitioned: RDD[(String, String)] = lines.map(line => (line(0), line(1))).partitionBy(new WebPatitioner)

    val reduced: RDD[((String, String), Int)] = partitioned.map((_, 1)).reduceByKey(_ + _)

    val grouped: RDD[(String, String, Int)] = reduced.groupBy(_._1._1).map(t => {
      val head = t._2.toList.sortBy(_._2).reverse.head
      (t._1, head._1._2, head._2)
    })

    val sorted = grouped.sortBy(_._3)

    sorted.saveAsTextFile("/spark_test/res2")
  }
}

class WebPatitioner extends Partitioner {
  override def numPartitions: Int = 2

  override def getPartition(key: Any): Int = if (key.toString.startsWith("www.")) 0 else 1
}
