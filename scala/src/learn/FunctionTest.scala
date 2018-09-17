package learn

object FunctionTest {
  def main(args: Array[String]): Unit = {
    val fun = extract(5)(0.0001) _
    val result = fun(5, 1)
    println(result)
  }

  /**
    * 求根柯里化函数
    *
    * @param square   几次根
    * @param accuracy 精度
    * @param value    开方数
    * @param forecast 预测值
    */
  def extract(square: Int)(accuracy: Double)(value: Double, forecast: Double): Double = {
    if (square == 0) {
      return 1
    }
    if (square == 1) {
      return value
    }
    if (square == -1) {
      return 1 / value
    }
    val squareAbs = if (square > 0) square else -square
    val accuracyAbs = if (accuracy > 0) accuracy else -accuracy

    var root = forecast
    var squareResult = root
    for (_ <- 1 until squareAbs) {
      squareResult *= root
    }

    while (value - squareResult > accuracyAbs || value - squareResult < -accuracyAbs) {
      println(root)
      root = (root + value / squareResult * root) / 2
      squareResult = root
      for (_ <- 1 until squareAbs) {
        squareResult *= root
      }
    }

    if (square > 0) root else 1 / root
  }
}
