package spark.sql

import org.apache.spark.sql.{Dataset, SparkSession}
import util.ScalaUtil

/**
  * 访问日志ip归属地统计排序
  */
object IpLocationSql {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder().appName("WordCountSql").master("local[*]")

    //获取session
    val session = builder.getOrCreate()

    //读取文件成DataSet
    val lines: Dataset[String] = session.read.textFile("/spark_test/words.txt")

    import session.implicits._

    val ipMapArr: Array[(String, (Long, Long))] = lines.map(line => {
      val arr = line.split("|")
      (arr(6), (arr(2).toLong, arr(3).toLong))
    }).collect()

    //设置ip归属地映射信息广播变量
    val ipMapBc = session.sparkContext.broadcast(ipMapArr)

    //注册自定义汉书
    session.udf.register("ip2Loacl", (ip: String) => {
      val long = ScalaUtil.ip2Long(ip)
      val ipMap: Array[(String, (Long, Long))] = ipMapBc.value
      var index = ScalaUtil.binarySearch(ipMapArr, (value: (String, (Long, Long))) => {
        val ipArea = value._2
        if (long < ipArea._1)
          -1
        else if (long > ipArea._2)
          1
        else
          0
      })
      (ipMap(index)._1, 1)
    })

    //读取访问文件ip信息
    val ips: Dataset[String] = session.read.textFile("/spark_test/accessLog.txt").map(_.split("|")(1))

    //创建临时视图
    ips.createTempView("V_IPS")

    //sql
    val result = session.sql("SELECT VALUE,COUNT(1) COUNTS FROM V_IPS GROUP BY VALUE ORDER BY COUNTS DESC")

    session.close()
  }
}
