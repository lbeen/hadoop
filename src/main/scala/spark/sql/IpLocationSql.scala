package spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
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

    session.udf.register("ip2Loacl", (ip:String) => {
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



    //IP归属地和数量统计
    val locationAndCount: RDD[(String, Int)] = ips.map(ip => {
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
    }).reduceByKey(_ + _)

    //排序
    val sorted = locationAndCount.sortBy(_._2)

    //保存
    sorted.saveAsTextFile("/spark_test/res2")

    //停止spack任务
    sc.stop()
  }
}
