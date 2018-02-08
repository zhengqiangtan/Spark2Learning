package com.func

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/2/8 0008.
  */
object CombinekeyTest {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("CombinekeyTest").setMaster("local[2]")
    val sc = new SparkContext(sparkconf)

    val initialScores = Array(("Fred", 88.0), ("Fred", 95.0), ("Fred", 91.0), ("Wilma", 93.0), ("Wilma", 95.0), ("Wilma", 98.0))
    val d1 = sc.parallelize(initialScores)

    type MVType = (Int, Double) //定义一个元组类型(科目计数器,分数)

    d1.combineByKey(
      score => (1, score),
      (c1: MVType, newScore) => (c1._1 + 1, c1._2 + newScore),
      (c1: MVType, c2: MVType) => (c1._1 + c2._1, c1._2 + c2._2)
    ).map {
      case (name, (num, socre)) => (name, socre / num)
    }.collect.foreach(println)
  }

}
