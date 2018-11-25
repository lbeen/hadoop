package spark.sql

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * scala版sparkWordCount
  *
  * @author 李斌
  */
object WordCountSql {

  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder().appName("WordCountSql").master("local[*]")

    //获取session
    val session = builder.getOrCreate()

    //读取文件成DataSet
    val lines: Dataset[String] = session.read.textFile("/spark_test/words.txt")

    //切割成单词
    import session.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))

    //sql
    val result = session.sql("SELECT VALUE,COUNT(1) COUNTS FROM V_WORDS GROUP BY VALUE ORDER BY COUNTS DESC")

    result.show()
  }
}
