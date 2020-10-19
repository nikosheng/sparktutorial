package spark

import org.junit.Test

class Closure {

  @Test
  def func: Unit = {
    val f: Int => Double = closure()
    println(f(1))
  }

  def closure(): Int => Double = {
    val PI = Math.PI
    val f = (r:Int) => Math.pow(r, 2) * PI
    f
  }
}
