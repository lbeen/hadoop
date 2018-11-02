package spark

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object WebLogMax {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("WebLogByHost").setMaster("local[*]"))
    //读取符合要求的行
    val lines = sc.textFile("/spark_test/weblog2.txt").map(_.split("\t"))

//    lines.map((_(0),_(1))).pa
  }
}

class WebPatitioner extends Partitioner{

}
