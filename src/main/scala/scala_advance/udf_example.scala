package scala_advance

import org.apache.spark.sql.SparkSession

case class table(
   TABLE_CATALOG:String  ,  TABLE_SCHEMA :String , TABLE_NAME: String, TABLE_TYPE:String, ENGINE:String,
   VERSION:Int,
   ROW_FORMAT:String,
   TABLE_ROWS:Int, AVG_ROW_LENGTH:Int, DATA_LENGTH: Int,MAX_DATA_LENGTH:Int, INDEX_LENGTH:Int, DATA_FREE:Int,
   AUTO_INCREMENT:String, CREATE_TIME:String, UPDATE_TIME:String, CHECK_TIME:String,
  TABLE_COLLATION:String, CHECKSUM:String, CREATE_OPTIONS:String, TABLE_COMMENT:String
)


object udf_example {

  def main ( args:Array[String]):Unit={
    val session = SparkSession.builder().master("local").appName("dfapi").getOrCreate()

    val DF= session.read.format("csv").option("delimiter","|").option("inferschema","true").option("header","true").load("/Users/abhinavkumar/Documents/Hadoop/notepadplus/table_data.txt")

    val udf_new= (( a:Double)=> a*2)

    session.udf.register("doubleudf", udf_new)

    import session.implicits._

   // val DF1= DF.filter(x=> !x.contains("TABLE_SCHEMA")).map(x=> x.split(","))
    //  .map(x=> table(x(0),x(1),x(2),x(3),x(4),x(5).toInt
   // , x(6),x(7).toInt,x(8).toInt, x(9).toInt, x(10).toInt , x(11).toInt, x(12).toInt,
    //    x(13) , x(14), x(15),x(16), x(17), x(18), x(19), x(20) )).toDF

  //  val DF2= DF.filter(x=> !x.contains("TABLE_SCHEMA")).asInstanceOf[table]


    DF.printSchema()
    DF.registerTempTable ("table_table")
    DF.cache()

    session.sql("select doubleudf(VERSION) from table_table").show(10)



  }
}
