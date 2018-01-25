package com.spark2.sparksql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 访问日志转换工具类
  * Created by Administrator on 2018/1/6 0006.
  */
object AccessConvertUtil {
  //定义输出的字段
  val struct=StructType(
    Array(
      StructField("url",StringType),
      StructField("cmsType",StringType),//课程类型
      StructField("cmsId",LongType),  //课程id
      StructField("traffic",LongType),
      StructField("ip",StringType),
      StructField("city",StringType),
      StructField("time",StringType),
      StructField("day",StringType)
    )
  )


  def parseLog(log:String) ={

    try{

      val splits=log.split("\t")

      val url=splits(1)
      val traffic=splits(2).toLong
      val ip=splits(3)

      //http://www.imooc.com/video/4500
      val domain="http://www.imooc.com/"
      val cms=url.substring(url.indexOf(domain)+domain.length)

      val cmsTypeId=cms.split("/")

      var cmsType=""
      var cmsId=0l
      if (cmsTypeId.length>1){
        cmsType=cmsTypeId(0)
        cmsId=cmsTypeId(1).toLong
      }

//      val city=IpUtils.getCity(ip)
      val city="上海市"

      val time=splits(0)
      val day=time.substring(0,10).replaceAll("-","")//20170511


      //完全和上面定义的对应
      Row(url,cmsType,cmsId,traffic,ip,city,time,day)

    }catch {
      case e:Exception=>Row(0)//报错用0代替
    }


  }

}
