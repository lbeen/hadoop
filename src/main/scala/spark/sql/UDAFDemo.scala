package spark.sql

import java.lang

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object UDAFDemo {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder().appName("UDAFDemo").master("local[1]")

    //获取session
    val session = builder.getOrCreate()

    val df: Dataset[lang.Long] = session.range(1, 10)

    df.show()

    session.udf.register("GM", new UDAFFun)

    df.createTempView("V_NUM")

    val result = session.sql("SELECT GM(ID) RESULT FROM V_NUM")

    result.show()
  }
}

class UDAFFun extends UserDefinedAggregateFunction {
  //UDAF与DataFrame列有关的输入样式，StructFieild的名字并没有特别要求，完全可以认为是两个内部结构的列名占位符
  //至于UDAF具体要求操作DataFrame的那个列，取决于调用者，但前提条件必须符合事先的设置
  override def inputSchema: StructType = StructType(List(StructField("value", DoubleType)))

  //定义存储聚合行数运算时产生的中间数据姐股票的Schema
  override def bufferSchema: StructType = StructType(List(
    StructField("count", LongType),
    StructField("product", DoubleType)
  ))

  //标明了UDAF函数的返回值类型
  override def dataType: DataType = DoubleType

  //用于标记针对给定的一组输入，UDAF是否总是生产相同的结果，一般选择true
  override def deterministic: Boolean = true

  //对聚合运算中间结果的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 1.0
  }

  //每行数据都要执行update
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    buffer(1) = buffer.getAs[Double](1) * input.getAs[Double](0)
  }

  //负责合并两个聚合运算的buffer，在将其存储到MutableAggregationBuffer中
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
  }

  //完成对聚合buffer值的运算，得到最后的结果
  override def evaluate(buffer: Row): Any = {
    math.pow(buffer.getDouble(1), 1.toDouble / buffer.getLong(0))
  }
}
