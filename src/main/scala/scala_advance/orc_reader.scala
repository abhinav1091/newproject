package scala_advance

import org.apache.spark.sql.SparkSession

object orc_reader {

  def main( args:Array[String]):Unit= {

    val session = SparkSession.builder().appName("word_counter").master("local").getOrCreate()

    val df_reader= session.read.format("orc").load("/Users/abhinavkumar/Documents/Hadoop/notepadplus/TestVectorOrcFile.orc")

    df_reader.foreach(x=>println(x))
  }
}
