package spark.sql

import java.net.URL

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * scala sql版网站访问日志统计
  */
object WebLog2 {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder().appName("WebLog2Sql").master("local[*]")

    //获取session
    val session = builder.getOrCreate()

    //从session获取sparkcontext
    val sc = session.sparkContext

    //读取符合要求的行
    val lines = sc.textFile("/spark_test/weblog.txt").map(_.split("\t")).filter(_.length == 6)

    //读取数据生成rowRDD
    val row = lines.map(line => Row(line(0), line(1), new URL(line(5)).getHost))

    //表结构
    val structType = StructType {
      Array(StructField("TIME", StringType), StructField("FWID", StringType), StructField("HOST", StringType))
    }

    //创建dataframe
    val dataframe = session.createDataFrame(row, structType)

    //创建零时视图
    dataframe.createTempView("V_WEB")

    //sql
    val result = session.sql("SELECT HOST,COUNT(1) COUNTS FROM V_WEB GROUP BY HOST ORDER BY COUNTS DESC LIMIT 50")

    //写入json
    result.write.json("/spark_test/res2")

    //停止spack任务
    session.close()
  }
}
