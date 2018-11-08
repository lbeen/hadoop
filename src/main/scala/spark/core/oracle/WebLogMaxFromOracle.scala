package spark.core.oracle

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}
import util.DBUtils

/**
  * 排序求访问最多网站
  */
object WebLogMaxFromOracle {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("WebLogMaxFromOracle").setMaster("local[*]"))

    //从数据库中取数据
    val sql = "SELECT ID,WEB,COUNTS FROM (SELECT ROWNUM N,ID,WEB,COUNTS FROM WEB_LOG_1) WHERE N>=? AND N<=?"
    val jdbcRdd = new JdbcRDD(sc, DBUtils.getConnection, sql, 1, 98849, 2, rs => (rs.getString("WEB"), rs.getInt("COUNTS")))

    //求每个网站访问次数
    val reduced = jdbcRdd.reduceByKey(_ + _)

    //排序
    val sorted = reduced.sortBy(_._2, ascending = false)

    //格式化一下
    val formated = sorted.map(r => r._1 + "\t" + r._2)

    //保存到hdfs
    formated.saveAsTextFile("/spark_test/res4")
  }
}
