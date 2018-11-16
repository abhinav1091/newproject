package scala_advance

import org.apache.spark.sql.SparkSession

object xml_reader {

  def main( args:Array[String] ):Unit= {
    val session = SparkSession.builder().appName("xml_reader").master("local").getOrCreate()

    val reader= session.read.format("com.databricks.spark.xml").option("rowTag", "book").load("/Users/abhinavkumar/Documents/Hadoop/notepadplus/food_menu.xml").select("author","genre")

    reader.printSchema()

    reader.foreach(x=> println(x))
  }
  }
