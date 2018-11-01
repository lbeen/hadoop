package spark

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WebLogByHost {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("WebLog2"))
    //读取符合要求的行
    val lines = sc.textFile(args(0)).map(_.split("\t")).filter(_.length == 6)

    //网站和id
    val hostAndId = lines.map(line => (new URL(line(5)).getHost, line(1)))

    //取访问超过100的网站
    val filter = hostAndId.map(t => (t._1, 1)).reduceByKey(_+_).filter(_._2>100)

    //取访问超过100的网站的记录
    val record = hostAndId.join(filter)

    //格式化一下
    val formated = record.map(r => r._1 + "\t" + r._2._1 + "\t")

    //保存
    formated.saveAsTextFile(args(1))

    //停止spack任务
    sc.stop()
  }
}
