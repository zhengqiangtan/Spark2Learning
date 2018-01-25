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

    spark.sql("create table if not exists src (key int, value string)")
    spark.sql("load data local inpath '/home/hadoop/text/kv1.txt' into table src")
//    spark.sql("select * from src").show()
//    spark.sql("select count(*) from src").show()


//    scala> sqlDF.show
//    |  0|val_0|
//      |  0|val_0|
//      |  0|val_0|
//      |  2|val_2|
//      |  4|val_4|
//      |  5|val_5|
//      |  5|val_5|
//      |  5|val_5|
//      |  8|val_8|
//      |  9|val_9|
//      +---+-----+

    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"

    }
    stringsDS.show()
//      scala> stringsDS.show
//      |Key: 0, Value: val_0|
//      |Key: 0, Value: val_0|
//      |Key: 0, Value: val_0|
//      |Key: 2, Value: val_2|
//      |Key: 4, Value: val_4|
//      |Key: 5, Value: val_5|
//      |Key: 5, Value: val_5|
//      |Key: 5, Value: val_5|
//      |Key: 8, Value: val_8|
//      |Key: 9, Value: val_9|
//      +--------------------+

    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
//    spark.created
    recordsDF.createOrReplaceTempView("records")
    spark.sql("select * from records r join src s on r.key = s.key").show()

  }
}
//+---+------+---+------+
//|key| value|key| value|
//+---+------+---+------+
//|  2| val_2|  2| val_2|
//|  4| val_4|  4| val_4|
//|  5| val_5|  5| val_5|
//|  5| val_5|  5| val_5|
//|  5| val_5|  5| val_5|
//|  8| val_8|  8| val_8|
//|  9| val_9|  9| val_9|
//| 10|val_10| 10|val_10|
//| 11|val_11| 11|val_11|
//| 12|val_12| 12|val_12|
//| 12|val_12| 12|val_12|
//| 15|val_15| 15|val_15|
//| 15|val_15| 15|val_15|
//| 17|val_17| 17|val_17|
//| 18|val_18| 18|val_18|
//| 18|val_18| 18|val_18|
//| 19|val_19| 19|val_19|
//| 20|val_20| 20|val_20|
//| 24|val_24| 24|val_24|
//| 24|val_24| 24|val_24|
//+---+------+---+------+