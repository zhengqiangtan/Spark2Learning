package com.spark2.sparksql

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/7/22 0022.
  */
object SparkSQLDemo1 {
  case class Person(name:String , age:Long)
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("test1")
      .master("local")
      .config("spark.sql.warehouse.dir","C:\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._
    //读取json文件
//    val df=spark.read.json("c://spark-data/people.json")
//    df.show()
//    df.printSchema()
//    df.select("name").show()
//    df.select($"name", $"age" + 1).show()
//    df.filter($"age" > 21).show()
//    df.groupBy("age").count().show()

  //创建临时视图
//  df.createOrReplaceTempView("people")
//  val sqlDF=spark.sql("select name,age from people")
//    sqlDF.show()

    //DataSet:Typed操作
//    val caseClassDS=Seq(Person("xiaoqiang",26)).toDS()
//    caseClassDS.show()
//
//    val primitiveDS = Seq(1, 2, 3).toDS()
//    primitiveDS.map(_ + 1).collect()

    val path = "people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()

  }
}
