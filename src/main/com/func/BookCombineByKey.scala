package com.func

import org.apache.spark.{SparkConf, SparkContext}

case class Book(val name: String, val price: Int) {
  //卖书然后买一个十块钱的钱包来装钱
  def sellBook_buyWallet: Wallet = Wallet(price - 10)
}

case class Wallet(var money: Int) {
  //将不同的钱包的钱累加起来
  def addMoney(that: Book): Wallet = Wallet(money + that.price)
}

/**
  * http://blog.csdn.net/Gpwner/article/details/73349589
  * http://blog.csdn.net/jiangpeng59/article/details/52538254
  *
  * combineByKeyWithClassTag
  *
 def combineByKey[C](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C): RDD[(K, C)] = self.withScope {
    combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners)(null)

解释：
createCombiner: V => C ，这个函数把当前的值作为参数，此时我们可以对其做些附加操作(类型转换)并把它返回 (这一步类似于初始化操作)
mergeValue: (C, V) => C，该函数把元素V合并到之前的元素C(createCombiner)上 (这个操作在每个分区内进行)
mergeCombiners: (C, C) => C，该函数把2个元素C合并 (这个操作在不同分区间进行)

  }
  */
object BookCombineByKey {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("Book_Sell").setMaster("local[2]")
    val sc = new SparkContext(sparkconf)

    val bookRDD = sc.makeRDD(Array(
      ("数据结构", Book("大话数据结构", 50)),
      ("数据结构", Book("数据结构C++版本", 80)),
      ("数据结构", Book("数据结构Java版本", 50)),
      ("算法", Book("算法导论", 50)),
      ("算法", Book("算法基础", 20)),
      ("算法", Book("算法心得", 50)),
      ("操作系统", Book("深入理解操作系统", 30)),
      ("操作系统", Book("Linux操作系统", 70)),
      ("操作系统", Book("Windows操作系统", 50))
    ),2) //默认，分区数为2

    println("partitions:  "+bookRDD.partitions.size)

    val aggresult = bookRDD.combineByKeyWithClassTag(
      (book: Book) => book.sellBook_buyWallet,
      (w: Wallet, b: Book) => w.addMoney(b),
      (w1: Wallet, w2: Wallet) => {
        println("================ call ===============")
        Wallet(w1.money + w2.money)
      }
    ).collectAsMap().foreach(println)
  }
}


//(算法,Wallet(100))
//(操作系统,Wallet(140))
//(数据结构,Wallet(170))