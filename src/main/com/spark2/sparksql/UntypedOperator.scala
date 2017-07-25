package com.spark2.sparksql

import org.apache.spark.sql.SparkSession

/**
  * untyped弱类型操作：改造统计部门平均薪资
  * select ,where , groupby , agg , col , join
  * Created by Administrator on 2017/7/24 0024.
  */
object UntypedOperator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("UntypedOperator")
      .master("local")
      .config("spark.sql.warehouse.dir", "C:\\spark-warehouse")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val employee = spark.read.json("C:\\spark-data\\employee.json")
    val department = spark.read.json("C:\\spark-data\\department.json")


  employee
    .where("age>20")
      .join(department,$"depId"===$"id")
      .groupBy(department("name"),employee("gender"))
      .agg(avg(employee("salary")))
      .show()

    employee
      .select($"name",$"depId",$"salary")
      .where("age>30")
      .show()

  }
}
