package spark

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class TransformationScala {

  val conf = new SparkConf().setMaster("local[2]").setAppName("spark.TransformationScala")
  val sc = new SparkContext(conf)

  @Test
  def mapPartitions: Unit = {
    sc.parallelize(Seq(1,2,3,5,6), 2)
      .mapPartitions( iter => {
        iter.map(_*10)
      })
      .foreach(println)

    sc.stop()
  }

  @Test
  def mapValues ={
    sc.parallelize(Seq(1,2,3,4,5,6), 2)
        .map( item => {
          val index = Integer.toString(item)
          (index, item)
        })
        .mapValues(_ * 10)
        .foreach(println)

    sc.stop()
  }

  @Test
  def groupByKeys ={
    sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3)))
        .groupByKey(2)
        .foreach(println)

    sc.stop()
  }

}
