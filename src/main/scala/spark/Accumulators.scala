package spark

import org.apache.spark.{SparkConf, SparkContext}

object Accumulators {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Accumulators")
    val sc = new SparkContext(conf)
    val counter = sc.longAccumulator("counter")

    sc.parallelize(Seq(1,2,3,4,5))
      .foreach(x => {
        counter.add(x)
      })

    println(counter.value)

    sc.stop()
  }
}
