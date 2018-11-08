package spark.core

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

object WebLogByHost {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("WebLogByHost").setMaster("local[*]"))
    //读取符合要求的行
    val lines = sc.textFile("/spark_test/weblog.txt").map(_.split("\t")).filter(_.length == 6)

    //网站和id
    val hostAndId = lines.map(line => (new URL(line(5)).getHost, line(1)))

    //取访问超过100的网站
    val filter = hostAndId.map(t => (t._1, 1)).reduceByKey(_ + _).filter(_._2 > 100)

    //取访问超过100的网站的访问记录
    val record = hostAndId.join(filter)

    //格式化一下
    val formated = record.map(r => r._1 + "\t" + r._2._1)

    //保存
    formated.saveAsTextFile("/spark_test/res2")

    //停止spack任务
    sc.stop()
  }
}
