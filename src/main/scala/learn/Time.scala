package learn

/**
  * scala类练习
  */
class Time(var hours: Int, var minutes: Int) {
  //之本对象可见
  private[this] var assertionError = 0

  if (hours < 0 || hours > 23) {
    throw new Exception("hours error")
  }
  if (minutes < 0 || minutes > 59) {
    throw new Exception("minutes error")
  }

  def get(): Unit = {
    println(hours + ":" + minutes)
  }

  def `do`(): Unit = {
    if (minutes == 59) {
      minutes = 0
      if (hours == 23) {
        hours = 0
      } else {
        hours += 1
      }
    } else {
      minutes += 1
    }
    println(hours + ":" + minutes)
  }
}

object Test {
  def main(args: Array[String]): Unit = {
    var time = new Time(23, 58)
    time.get()
    time.`do`()
    time.`do`()
  }
}
