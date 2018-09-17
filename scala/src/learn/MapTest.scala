package learn

object MapTest {
  def main(args: Array[String]): Unit = {
    println(tuples)
  }

  /**
    * map初始化
    */
  def init(): Map[String, String] = {
    //不可变map
    var map = Map("1" -> "a", "2" -> "b", "3" -> "c")
    println(map)
    map = Map(("3", "c"), ("4", "d"), ("5", "e"))
    println(map)
    map
  }

  /**
    * map操作
    */
  def test(): Any = {
    //可变map
    var map = scala.collection.mutable.Map(("1", "a"))
    println(map)

    //添加
    map += (("2", "b"))
    map.put("2", "b")
    map.put("3", "c")
    println(map)

    //删除
    map -= "2"
    map.remove("2")
    println(map)


    //获取
    println(map("1"))
    println(map.get("1"))
    println(map.contains("2"))
    println(map.getOrElse("2", "null1"))

    for ((k, v) <- map) print(k + ":" + v + ",")
  }

  /**
    * 元组：一组不同元素的集合
    */
  def tuples: Any = {
    val one = Array("a", "b", "c")
    val two = Array(1, 2, 3)

    val tuples = one zip two
    println(tuples)

    println(one zip two toMap)
  }

}
