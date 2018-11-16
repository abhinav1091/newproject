package scala_advance

import org.apache.spark.sql.SparkSession


object parquet_format {

  def main( args:Array[String]):Unit= {

    val session = SparkSession.builder().appName("parquet_reader").master("local").getOrCreate()

    val df_reader= session.read.format("parquet").load("/Users/abhinavkumar/Documents/Hadoop/notepadplus/userdata1.parquet").select("registration_dttm")
    import session.implicits._

    df_reader.printSchema()

    //df_reader.withColumn("id", when($"id" === 32, 123456).otherwise($"id")).show
    // df_reader.na.fill("id",Seq("id"))
    df_reader.foreach(x=>println(x))
  }
}
