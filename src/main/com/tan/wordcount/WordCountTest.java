package com.tan.wordcount;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

//  file:/D:/MyWorkspace/learn_workpace/Spark2Learning/spark-warehouse

public final class WordCountTest {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

//        if (args.length < 1) {
//            System.err.println("Usage: JavaWordCount <file>");
//            System.exit(1);
//        }

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount").master("local[2]")
//                .config("spark.sql.warehouse.dir", "C:\\spark-warehouse")
                .getOrCreate();



        JavaRDD<String> lines = spark.read().textFile("C:\\spark-data\\wordcount.txt").javaRDD();


        Dataset<String> dataset = spark.read().textFile("C:\\spark-data\\wordcount.txt");
//        dataset.na().fill("xxxx")
//        dataset.na().fill("zzz", new String[]{"aaa","bbb"});
//        dataset.na().drop()

//        JavaRDD<Row> text = spark.read().format("text").load("C:\\spark-data\\wordcount.txt").javaRDD();

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String,Integer>(s, 1);
                    }
                });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(
                new Function2<Integer, Integer, Integer>() {

                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        spark.stop();
    }
}