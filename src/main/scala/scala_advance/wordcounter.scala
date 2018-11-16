package scala_advance
import org.apache.spark.sql.SparkSession


object wordcounter {

  def main( args:Array[String] ):Unit={
    val session = SparkSession.builder().appName("word_counter").master("local").getOrCreate()

    val df_reader= session.read.textFile("/Users/abhinavkumar/Documents/Hadoop/notepadplus/words.txt").rdd
    val filter_df= df_reader.flatMap(x=> x.split(" ")).filter(x=> x.length!=3).map(x=> (x,1)).reduceByKey(_+_)

    filter_df.foreach(x=> println(x))

  }
}
