package com.spark2.sparksql
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Administrator on 2017/7/22 0022.
  */
object SparkHiveSQL {
  case class Record(key: Int, value: String)
  val warehouseLocation = "c://spark-warehouse"
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      //.master("local") //不支持本地模式
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()//开启hive支持
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    sql("LOAD DATA LOCAL INPATH 'kv1.txt' INTO TABLE src")
    sql("SELECT * FROM src").show()
    sql("SELECT COUNT(*) FROM src").show()

    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"

    }
    stringsDS.show()

    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")
    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()

  }
}
