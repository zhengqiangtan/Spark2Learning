package datasource

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object CustomDatasourceApp extends App {

  val conf = new SparkConf().setAppName("spark-custom-datasource")
  val spark = SparkSession.builder().config(conf).master("local").getOrCreate()
  spark.sparkContext.textFile("")

  val df = spark.read.format("datasource").load("data/")

  // Step 1 (Schema verification)
  df.printSchema()
  // Step 2 (Read data)
  df.show()
  // Step 3 (Write data)
  df.write
    .options(Map("format" -> "customFormat"))
    .mode(SaveMode.Overwrite)
    .format("datasource")
    .save("out/")

  // Step 4 (Column Pruning)
  df.createOrReplaceTempView("salaries")

  spark.sql("SELECT surname, salary FROM salaries").show()

  // Step 5 (Filter Pushdown)
  spark.sql("SELECT name, surname FROM salaries WHERE salary > 40000").show()

}


//root
//|-- name: string (nullable = true)
//|-- surname: string (nullable = true)
//|-- salary: integer (nullable = true)
//
//Trying to push down filters...
//Selecting only required columns...