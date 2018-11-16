package scala_advance

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object append_log_reader {

  def main( args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("stream").setMaster("local[2]")
    //val session = SparkSession.builder().appName("word_counter").master("local").getOrCreate()
    val ssc = new StreamingContext(conf, Seconds(3))

    val data = ssc.textFileStream("/Users/abhinavkumar/Documents/stream/")
    data.print()

    /* val res= data.foreachRDD(t=>
    {
      val test = t.map(line=> line.toString)
    }
    )*/



    // res.print()
    data.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
