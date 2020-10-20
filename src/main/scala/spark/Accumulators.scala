package spark

import org.apache.spark.sql.SparkSession

object Accumulators {
  def main(args: Array[String]): Unit = {
    val spark = new SparkSession.Builder()
        .appName("spark-accumulators")
        .getOrCreate()

    val sc = spark.sparkContext

    val counter = sc.longAccumulator("counter")

    sc.parallelize(Seq(1,2,3,4,5))
      .foreach(x => {
        counter.add(x)
      })

    println(counter.value)

    sc.stop()
  }
}
