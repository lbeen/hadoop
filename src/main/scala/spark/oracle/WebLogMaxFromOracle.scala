package spark.oracle

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import util.DBUtils

/**
  * 排序求每个网站访问最多的id
  */
object WebLogMax {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("WebLogMaxFromOracle.scala").setMaster("local[*]"))

    val sql = "SELECT ID,WEB,COUNTS FROM (SELECT ROWNUM N,ID,WEB,COUNTS FROM WEB_LOG_1) WHERE N>=? AND N<=?"
    val jdbcRdd: JdbcRDD[((String, String), Int)] = new JdbcRDD(sc, DBUtils.getConnection, sql, 1, 98849, 2, rs => ((rs.getString("ID"), rs.getString("WEB")), rs.getInt("COUNTS")))

    //以网站和id为key求访问此书
    val reduced = jdbcRdd.reduceByKey(_ + _)

    //分区
    val partitioned = reduced.map(t => (t._1._1, (t._1._2, t._2))).partitionBy(new WebPatitioner)

    //排序求每个网站访问最多的id
    val sorted = partitioned.mapPartitions(_.toList.groupBy(_._1).map(_._2.sortBy(_._2._2).reverse.head).toList.sortBy(_._2._2).reverseIterator)

    //格式化一下
    val formated = sorted.map(r => r._1 + "\t" + r._2._1 + "\t" + r._2._2)

    formated.saveAsTextFile("/spark_test/res10")
  }
}

/**
  * 自定义以网站分区
  */
class WebPatitioner extends Partitioner {
  override def numPartitions: Int = 2

  override def getPartition(key: Any): Int = if (key.toString.startsWith("www.")) 0 else 1
}
