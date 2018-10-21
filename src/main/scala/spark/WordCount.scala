package spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * scala版sparkWordCount
  *
  * @author 李斌
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("wordCount"))

    //读取文件并切割成单词
    val words = sc.textFile(args(0)).flatMap(_.split(" "))

    //计算每个单词数量
    val counted = words.map((_, 1)).reduceByKey(_ + _)

    //排序
    val sortd = counted.sortBy(_._2)

    //保存结果
    sortd.saveAsTextFile(args(1))

    //停止spack任务
    sc.stop()
  }
}
