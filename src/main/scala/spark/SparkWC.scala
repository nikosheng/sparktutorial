package spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkWC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkWC")
    conf.setMaster("local")
    conf.set("spark.driver.host", "127.0.0.1")
    val sc = new SparkContext(conf)

    val path = this.getClass.getClassLoader.getResource("./file.txt").getPath
    sc.textFile(path).flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .map(_.swap)
      .sortByKey(false)
      .map(_.swap)
      .foreach(println)
    sc.stop()
  }
}
