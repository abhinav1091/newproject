package scala


import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object partitioner_exmp {

  def main(args:Array[String]) {
//    val sparkConf = new SparkConf ().setAppName ("SparkSessionZipsExample").setMaster ("local")
 //   val sc = new SparkContext (sparkConf)
 //    val sqlContext = new SQLContext (sc)

    val session = SparkSession.builder().master("local").appName("partitioner").enableHiveSupport().getOrCreate()
    import session.implicits._
    session.sql ("set spark.sql.shuffle.partitions=10")
    val x = (1 to 1000000).map (x => ( x % 32,x )).toDF("id", "Value")
    println(x.rdd.getNumPartitions)
    val z= x.rdd.repartition(32).map{case Row(val1: Int, val2:Int) => (val1,val2)}.toDF("id","value")
    println(z.rdd.getNumPartitions)
    z.printSchema()

    val y= z.groupBy("id").count()
    y.show(50)


    //x.createTempView ("hello")
    //val y = session.sql("select count(*) from hello group by value")
    println(y.rdd.getNumPartitions)
  }

}
