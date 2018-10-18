package learn

object FunctionTest {
  def main(args: Array[String]): Unit = {
    def accuracy(accuracy: Double): (Double, Double) => Boolean = (value: Double, root: Double) => {
      val result = value - root * root
      if (result >= 0) result > accuracy else result < -accuracy
    }

    val fun = square(accuracy(0.0001)) _

    println(fun(3, 1))
  }

  /**
    * 求根柯里化函数
    *
    * @param accuracy 精度判断函数
    * @param value    开方数
    * @param forecast 预测值
    */
  def square(accuracy: (Double, Double) => Boolean)(value: Double, forecast: Double): Double = {
    var root = forecast
    while (accuracy(value, root)) {
      println(root)
      root = (root + value / root) / 2
    }
    root
  }
}
