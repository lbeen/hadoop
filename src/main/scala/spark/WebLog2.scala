package spark

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * scala版搜狗访问日志统计
  */
object WebLog2 {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("WebLog2"))

    //读取符合要求的行
    val lines = sc.textFile(args(0)).map(_.split("\t")).filter(_.length == 6)

    //id和网址组成tuple作为key，统计每个人每个网站访问次数

//    val countedByIdAndWeb = lines.map(line => {
//      ((line(1), new URL(lines(5)).getHost), 1)
//    }).reduceByKey(_ + _)

    val countedByIdAndWeb = lines.map(line => ((line(1), new URL(lines(5)).getHost), 1)).reduceByKey(_ + _)

    //按网站分组
    var groupByWeb = countedByIdAndWeb.groupBy(_._1._2)

    //取每个网站最单人访问最多的数据
    val max = groupByWeb.mapValues(_.toList.sorted.reverse.take(1)).map(value => value._2.head)

    //过滤倒序
    var desc = false
    val result = max.filter(_._2 > 5).sortBy(_._2, desc)

    //保存结果
    result.saveAsTextFile(args(1))

    //停止spack任务
    sc.stop()
  }
}