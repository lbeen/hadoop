package spark.sql.oracle

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object FilterWeb {

  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder().appName("WebLog2Sql").master("local[*]")

    //获取session
    val session = builder.getOrCreate()

    //数据库信息
    val dbInfo = scala.collection.mutable.Map("url" -> "jdbc:oracle:thin:@192.168.1.110:1521:orcl",
      "driver" -> "oracle.jdbc.driver.OracleDriver",
      "dbtable" -> "WEB_LOG_1",
      "user" -> "test",
      "password" -> "123")

    //读取数据库中的log信息
    val logs: DataFrame = session.read.format("jdbc").options(dbInfo).load()

    //过滤
    val filtered: Dataset[Row] = logs.where(logs.col("counts") > 10)


    //保存结果
    dbInfo.put("dbtable", "WEB_LOG_2")
    filtered.write.format("jdbc").mode("overwrite").options(dbInfo).save()

    session.close()
  }

}
