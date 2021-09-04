import com.scala.WilsonScore

object TestWilsonScore {
  def main(args: Array[String]): Unit = {
    val score = new WilsonScore()
    val res = score.evaluate(10, 100, 2.0)
    println(res)

  }


}
