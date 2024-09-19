package com.availity.spark.provider

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object ProviderRoster {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    val providerDataDF = spark.read.option("header", true).option("delimiter", "|").csv("data/providers.csv")
    providerDataDF.createOrReplaceTempView("providerData")

    val visitsDataDF = spark.read.schema("visit_id string,provider_id string,visit_dt date").csv("data/visits.csv")
    visitsDataDF.createOrReplaceTempView("visitsData")

    val resultDf1 = spark.sql(
      """select tbl1.provider_id,
                           first(first_name) as first_name ,first(middle_name) as middle_name,
                           first(last_name) as last_name,first(provider_specialty) as provider_specialty,
                           count(*) as count
                           from providerData tbl1 inner join visitsData tbl2
                           on tbl1.provider_id=tbl2.provider_id
                           group by tbl1.provider_id""")
    resultDf1.write.mode(SaveMode.Overwrite).partitionBy("provider_specialty").json("data/output/result1")

    val resultDf2 = spark.sql(
      """select provider_id,DATE_FORMAT(visit_dt,"yyyy-MM") as visit_month,count(*) as count
          from visitsData
      group by provider_id,DATE_FORMAT(visit_dt,"yyyy-MM")""")
    resultDf2.write.mode(SaveMode.Overwrite).json("data/output/result2")


  }

}
