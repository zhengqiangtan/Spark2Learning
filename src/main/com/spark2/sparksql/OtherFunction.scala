package com.spark2.sparksql

import org.apache.spark.sql.SparkSession

/**
  * 常用函数：
  * http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$
  * 日期函数：current_date、current_timestamp
     数学函数：round
     随机函数：rand
     字符串函数：concat、concat_ws
     自定义udf和udaf函数
  *
  */
object OtherFunction {
  
  def main(args: Array[String]) {
    val spark = SparkSession
        .builder()
        .appName("OtherFunction") 
        .master("local") 
        .config("spark.sql.warehouse.dir", "C:\\spark-warehouse")
        .getOrCreate()
    
    import spark.implicits._
    import org.apache.spark.sql.functions._
    
    val employee = spark.read.json("C:\\spark-data\\employee.json")
    val department = spark.read.json("C:\\spark-data\\department.json")

    employee
        .select(employee("name"), 
          current_date(), 
          current_timestamp(), 
          rand(), 
          round(employee("salary"), 2), 
          concat(employee("gender"), employee("age")), 
          concat_ws("|", employee("gender"), employee("age")))  
        .show()  
  }
  
}
//output:
//+------+--------------+--------------------+--------------------------+----------------+-------------------+-------------------------+
//|  name|current_date()| current_timestamp()|rand(-4172913808826664553)|round(salary, 2)|concat(gender, age)|concat_ws(|, gender, age)|
//+------+--------------+--------------------+--------------------------+----------------+-------------------+-------------------------+
//|   Leo|    2017-07-24|2017-07-24 16:18:...|        0.4604555369072717|         20000.0|             male25|                  male|25|
//|   Leo|    2017-07-24|2017-07-24 16:18:...|        0.8511179633057339|         20000.0|             male25|                  male|25|
//| Marry|    2017-07-24|2017-07-24 16:18:...|        0.4499648009722437|         25000.0|           female30|                female|30|
//|  Jack|    2017-07-24|2017-07-24 16:18:...|        0.2850395593693583|         15000.0|             male35|                  male|35|
//|   Tom|    2017-07-24|2017-07-24 16:18:...|       0.47535812445310777|         18000.0|             male42|                  male|42|
//|Kattie|    2017-07-24|2017-07-24 16:18:...|        0.9704869672476799|         21000.0|           female21|                female|21|
//|   Jen|    2017-07-24|2017-07-24 16:18:...|        0.9732769661984227|         28000.0|           female30|                female|30|
//|   Jen|    2017-07-24|2017-07-24 16:18:...|        0.7458270065725604|         8000.87|           female19|                female|19|
//+------+--------------+--------------------+--------------------------+----------------+-------------------+-------------------------+