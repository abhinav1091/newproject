package main.scala

import org.apache.spark.HashPartitioner
import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.functions.{max, min}


object cricket {

  def main(args: Array[String]): Unit = {



    val session = SparkSession.builder().master("local").appName("cricket consolidator").enableHiveSupport().getOrCreate()
    val sessionreader = session.read


    val orginalDF = sessionreader.option("header", "true")
      .option("inferschema", value = true)
      .csv("/Users/abhinavkumar/Downloads/air-quality-madrid/csvs_per_year/*.csv")

    val originalDFcols = orginalDF.select(orginalDF.col("CO"), orginalDF.col("Station"))

    originalDFcols.createOrReplaceGlobalTempView("sample_data")

    val orginalDF2 = sessionreader.option("header", "true")
      .option("inferschema", value = true)
      .csv("/Users/abhinavkumar/Downloads/air-quality-madrid/stations.csv")

    val partitioner = new partitioner_custom(5)

    val originalDF2cols = orginalDF2.select(orginalDF2.col("id"), orginalDF2.col("name"))


    val resultDF = originalDFcols.join(originalDF2cols, originalDFcols.col("Station") === originalDF2cols.col("id"), "inner")
      .repartition(originalDFcols.col("Station"))

    val finalresult1= resultDF.select( resultDF.col("id")  ,resultDF.col("CO").cast(FloatType) )
      .groupBy(resultDF.col("id")).avg("CO")

    finalresult1.show(20)
    resultDF.write.format("orc").partitionBy("id").mode("overwrite").saveAsTable("Stations")

    session.sql("select count(*) from Stations").show(100)

    val resultDF2 = originalDFcols.join(originalDF2cols, originalDFcols.col("Station") === originalDF2cols.col("id"), "inner")
      .repartition(originalDFcols.col("Station"))

    val finalresult2= resultDF.select( resultDF.col("id")  ,resultDF.col("CO").cast(FloatType) )
      .groupBy(resultDF.col("id")).avg("CO")

    finalresult2.show(20)
    resultDF2.write.format("orc").partitionBy("id").mode("append").saveAsTable("Stations")

    session.sql("select count(*) from Stations").show(100)

    session.stop()
  }
}