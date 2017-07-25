package com.spark2.sparksql

import org.apache.spark.sql.SparkSession

/**
  * 聚合函数：涉及到分组聚合要进行shuffle
  * avg , sum , max ,min , count , countDistinct
  * collect_list , collect_set
  */
object AggregateFunction {
  
  def main(args: Array[String]) {
    val spark = SparkSession
        .builder()
        .appName("AggregateFunction") 
        .master("local") 
        .config("spark.sql.warehouse.dir", "C:\\spark-warehouse")
        .getOrCreate()
    
    import spark.implicits._
    import org.apache.spark.sql.functions._
    
    val employee = spark.read.json("C:\\spark-data\\employee.json")
    val department = spark.read.json("C:\\spark-data\\department.json")
    
//    employee
//        .join(department, $"depId" === $"id")
//        .groupBy(department("name"))
//        .agg(avg(employee("salary")), sum(employee("salary")), max(employee("salary")), min(employee("salary")), count(employee("name")), countDistinct(employee("name")))
//        .show()

    // collect_list，就是将一个分组内，指定字段的值都收集到一起，不去重
    // collect_set，同上，但是唯一的区别是，会去重
    // collect_list和collect_set，都用于将同一个分组内的指定字段的值串起来，变成一个数组
    // 常用于行转列
    // 比如说
    // depId=1, employee=leo
    // depId=1, employee=jack
    // depId=1, employees=[leo, jack]

    employee
      .groupBy(employee("depId"))
      .agg(collect_set(employee("name")), collect_list(employee("name")))
      .collect()
      .foreach(println(_))


  }
  
}

//+--------+------------------+-----------+-----------+-----------+-----------+--------------------+
//|    name|       avg(salary)|sum(salary)|max(salary)|min(salary)|count(name)|count(DISTINCT name)|
//+--------+------------------+-----------+-----------+-----------+-----------+--------------------+
//|Tec Dept|18333.333333333332|      55000|      20000|      15000|          3|                   2|
//|Fin Dept|20333.333333333332|      61000|      28000|       8000|          3|                   2|
//| HR Dept|           19500.0|      39000|      21000|      18000|          2|                   2|
//+--------+------------------+-----------+-----------+-----------+-----------+--------------------+

//
//[1,WrappedArray(Jack, Leo),WrappedArray(Leo, Leo, Jack)]
//[3,WrappedArray(Tom, Kattie),WrappedArray(Tom, Kattie)]
//[2,WrappedArray(Marry, Jen),WrappedArray(Marry, Jen, Jen)]