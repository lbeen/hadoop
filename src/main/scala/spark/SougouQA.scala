package spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * scala版搜狗访问日志统计
  */
object SougouQA {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("SougouQA"))

    //读取符合要求的行
    val lines = sc.textFile(args(0)).map(_.split("\t")).filter(_.length == 6)

    //统计每个id访问数
    val counted = lines.map(line => (line(1), 1)).reduceByKey(_ + _)

    //访问数排序后去除访问数大于20的数据
    val sortd = counted.sortBy(_._2).filter(_._2 > 100)

    //保存结果
    sortd.saveAsTextFile(args(1))

    //停止spack任务
    sc.stop()
  }
}