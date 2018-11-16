package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

//import scala.reflect.getClass

object movie {

  def main( args:Array[String]):Unit = {

    val session = SparkSession.builder().master("local").appName(" movie rating").getOrCreate()
    val session_Reader= session.read

    val NEWDF= session.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/information_schema")
                                             .option("driver","com.mysql.jdbc.Driver")
                                            .option("dbtable", "TABLES")
        .option("user","root")
        .option("password","abhinav1991")
                                           .load()
    NEWDF.show()
    val movies= session_Reader.option("header", "true").option("inferschema", value=true)
      .csv("/Users/abhinavkumar/Downloads/ml-20m/movies.csv")

    val ratings= session_Reader.option("header", "true").option("inferschema", value=true)
      .csv("/Users/abhinavkumar/Downloads/ml-20m/ratings.csv")



    val joinres =  ratings.join(movies,ratings.col("movieId")===movies.col("movieId"), "inner")
      .select(ratings.col("movieId"),ratings.col("rating"),movies.col("title"), movies.col("movieId"))
      .groupBy(ratings.col("movieId"),movies.col("title"))
      .agg(avg(ratings.col("rating")).alias("avg_rating")).orderBy(desc("avg_rating"))
     .filter(col("avg_rating")===3)
      .show(100)




    //joinres.printSchema()

    //joinres.select(ratings.col("movieId"),joinres.col("title"),ratings.col("rating")).groupBy("movieId")
     // .agg(avg("rating").alias("avg_rating")).orderBy(desc("avg_rating"))

  }


}
