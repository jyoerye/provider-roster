package com.availity.spark.provider

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, StringType, StructType}
import org.apache.spark.sql.functions.{array, avg, col, collect_list, count, lit, month}

object ProviderRoster  {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[2]")
    val spark=SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    println("I m reading csv")
   import spark.implicits._
    val df1=spark.read.csv("data/providers.csv")
    df1.show(10,false)
  }

}
