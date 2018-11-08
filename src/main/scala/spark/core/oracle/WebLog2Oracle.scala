package spark.core.oracle

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.nutz.dao.impl.sql.NutSql
import util.DBUtils

/**
  * 网站访问日志统计导入oracle
  */
object WebLog2Oracle {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("WebLog2Oracle").setMaster("local[*]"))

    //读取符合要求的行
    val lines = sc.textFile("/spark_test/weblog.txt").map(_.split("\t")).filter(_.length == 6)

    //id和网址组成tuple作为key，统计每个人每个网站访问次数
    val countedByIdAndWeb = lines.map(line => ((line(1), new URL(line(5)).getHost), 1)).reduceByKey(_ + _)

    //按网站分组
    var groupByWeb = countedByIdAndWeb.groupBy(_._1._2)

    //取每个网站最单人访问最多的数据
    val max: RDD[((String, String), Int)] = groupByWeb.mapValues(_.toList.sorted.reverse.take(1)).map(value => value._2.head)

    //分区导入
    max.foreachPartition(data2Oracle)

    sc.stop()
  }

  /**
    * 数据导入oracle
    *
    * @param it 数据迭代器
    */
  def data2Oracle(it: Iterator[((String, String), Int)]): Unit = {
    val dao = DBUtils.dao()
    val sql = new NutSql("INSERT INTO WEB_LOG_1 (ID,WEB,COUNTS) VALUES (@id,@web,@counts)")
    for (t <- it) {
      sql.params().set("id", t._1._1).set("web", t._1._2).set("counts", t._2)
      sql.addBatch()
    }
    dao.execute(sql)
  }
}