package learn

import scala.collection.mutable.ArrayBuffer

object ArrayTest {
  def main(args: Array[String]): Unit = {
    var array = new Array[Int](10)
    array(7) = 57
    //0 0 0 0 0 0 0 57 0 0
    var b = array.toBuffer
    b.remove(2, 2) //0 0 0 0 0 57 0 0
    for (i <- 1 to(b.length, 2)) print(b(i))
    //0 0 57 0
  }

  /**
    * 先定义在赋值
    */
  def arrayInit1(): Array[Int] = {
    var array = new Array[Int](6)
    for (i <- 0 to 5) array(i) = i + 5
    array
  }

  /**
    * 定义就赋值
    */
  def arrayInit2(): Array[Int] = {
    Array(1, 2, 3, 4)
  }

  /**
    * 动态数组
    */
  def arrayBufferTest(): Any = {
    var array = new ArrayBuffer[Int]()

    //添加元素
    for (i <- 1 to 5) array += i
    for (elem <- array) print(elem + ",")
    println()
    //添加数组
    array ++= Array(6, 7)
    for (elem <- array) print(elem + ",")
    println()
    //移除开始结束元素
    array.trimStart(2)
    array.trimEnd(2)
    for (elem <- array) print(elem + ",")
    println()
    //插入元素
    array.insert(2, 4, 4, 5)
    for (elem <- array) print(elem + ",")
    println()
    //移除元素
    array.remove(2, 4)
    for (elem <- array) print(elem + ",")
    println()
  }

  /**
    * 数组遍历
    */
  def each(array: Array[Int]): Any = {
    //普通遍历
    for (elem <- array) print(elem + ",")
    println()
    //角标遍历
    for (i <- array.indices) print(i + ":" + array(i) + ",")
    println()
    //每两个遍历
    for (i <- 0 to(array.length - 1, 2)) print(array(i))
    println()
    //倒序便利遍历
    for (i <- (0 to(array.length - 1, 2)).reverse) print(array(i))
    println()
  }
}
