package spark.sql

import java.net.URL

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * scala版网站访问日志统计
  */
object WebLog2 {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder().appName("WebLog2Sql").master("local[*]")

    val session = builder.getOrCreate()



    val sc = session.sparkContext

    //读取符合要求的行
    val lines = sc.textFile("/spark_test/weblog.txt").map(_.split("\t")).filter(_.length == 6)

    val row = lines.map(line => Row(line(0), line(1), new URL(line(5)).getHost))

    val structType = StructType {
      Array(StructField("TIME", StringType), StructField("FWID", StringType), StructField("HOST", StringType))
    }

    val dataframe = session.createDataFrame(row, structType)

    dataframe.createTempView("V_WEB")

    val result = session.sql("SELECT HOST,COUNT(1) COUNTS FROM V_WEB GROUP BY HOST ORDER BY COUNTS DESC")

    result.write.json("/spark_test/weblog.txt")
    //停止spack任务
    session.close()
  }
}
