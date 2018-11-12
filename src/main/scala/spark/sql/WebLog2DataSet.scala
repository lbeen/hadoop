package spark.sql

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * scala sql版网站访问日志统计
  */
object WebLog2DataSet {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder().appName("WebLog2DataSet").master("local[*]")

    //获取session
    val session = builder.getOrCreate()

    import session.implicits._
    val lines: Dataset[Array[String]] = session.read.textFile("/spark_test/weblog.txt").map(_.split("\t")).filter(_.length == 6)


  }
}
