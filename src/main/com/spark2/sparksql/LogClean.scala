package com.spark2.sparksql

import org.apache.spark.sql.{SaveMode, SparkSession}


object LogClean {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder()
      .config("spark.sql.warehouse.dir","C:\\spark-warehouse")
      .appName("SparkJobClean")
      .master("local[2]")
      .getOrCreate()

    val peopleDF = spark.read.json("c://spark-data//people.json")
    peopleDF.show()
    peopleDF.write.mode(SaveMode.Overwrite).parquet("c://spark-data//data")

    spark.stop()

  }
}
