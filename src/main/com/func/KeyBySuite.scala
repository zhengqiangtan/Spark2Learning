package com.func
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.instrumentation.Timers._



/**
  *keyby zipwithIndex sortBykey
  * https://github.com/bigdatagenomics/adam
  */
object KeyBySuite {
  def main(args: Array[String]) {
    keybyTest()
  }

  def keybyTest() = SortReads.time {
    val conf = new SparkConf().setAppName(this.getClass().getSimpleName().filter(!_.equals('$'))).setMaster("local[4]")
    val sc = new SparkContext(conf)
    //    val rdd1 = sc.parallelize(Array(("a", 2, 3), ("b", 2, 5), ("a", 4, 100), ("b", 1, 200), ("a", 1, 1)))
    val rdd1 = sc.parallelize(Array(("ab", 2, 3), ("bb", 2, 5), ("ac", 4, 100), ("bb", 1, 200), ("ac", 1, 1)))
    val rdd2 = rdd1.keyBy(each => (each._1, each._2)).sortByKey()
    println("init:")
    rdd1.foreach(println)

    println("keyby not zipWithIndex:")
    rdd2.foreach(println)

    println("keyby value:")
    rdd2.map(_._2).foreach(println)

    println("zipWithIndex:")
//    val rdd3 = rdd1.keyBy(each => (each._1, each._2)).sortByKey().map(_._2).collect().zipWithIndex
    val rdd3 = rdd2.map(_._2).collect().zipWithIndex
    rdd3.foreach(println)

    println("sort:")
    rdd3.map(_._1).foreach(println)


    sc.stop
  }
}