package util

object ScalaUtil {
  /**
    * 二分查找
    *
    * @param array    查找数组
    * @param checkFun 判断大小函数
    * @tparam T 数组类型
    * @return 查找结果数组索引
    */
  def binarySearch[T](array: Array[T], checkFun: T => Int): Int = {
    var low = 0
    var hight = array.length - 1
    while (low <= hight) {
      val mid = (hight + low) / 2
      val checkR = checkFun(array(mid))
      if (checkR == 0) {
        return mid
      } else if (checkR < 0) {
        hight = mid - 1
      } else {
        low = mid + 1
      }
    }
    -1
  }

  /**
    * ip转long
    */
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8
    }
    ipNum
  }
}