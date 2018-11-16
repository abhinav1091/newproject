package scala_advance

import org.apache.spark.sql.SparkSession

object cassendra {

  def main( args: Array[String]):Unit= {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("Test").enableHiveSupport()
      .config("spark.cassandra.connection.host","localhost")
      .config("spark.cassandra.connection.port", "9042")
      .getOrCreate();

    //SparkSession.builder().master("local").appName("cricket consolidator").enableHiveSupport().getOrCreate()

    val employee = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table"-> "emp", "keyspace"-> "abhinav"))
      .load()



    employee.printSchema()
    employee.show(10)
    
    employee.createOrReplaceTempView("emp1")
    spark.sqlContext.sql("select * from emp1").show()
  }

}
