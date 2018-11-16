package scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.WrappedArray

case class golilockscase( Happiness: Double, Niceness:Double, Softness:Double, Sweetness:Double)

object golilocks {

  def main(args:Array[String]):Unit= {

    val session = SparkSession.builder().master("local").appName("Goldilock").getOrCreate()

    import session.implicits._

    val sessionreader= session.read

    val rdd = sessionreader.textFile("/Users/abhinavkumar/Documents/Hadoop/notepadplus/goldilocks.txt").rdd

    //rdd.map(line=> line.split('|')).map(x=> (x(0), x(1),x(2),x(3))).toDF("Happiness","Niceness","Softness","Sweetness")

    val DF=  rdd.map(line=> line.split('|')).map(x=> golilockscase(x(0).toDouble, x(1).toDouble,x(2).toDouble,x(3).toDouble)).toDF()

    DF.printSchema()
    val x:List[Long]= List(2)
    val res= findRankStatistics(DF,List(2,3))

    res.foreach(x=> println(x._1 + " " + x._2))
    val y= res.map{case(x,iter)=> (iter.toArray)}
    y.map{case(y)=> println((y(0)+ " " + y(1)))}

  }

  def findRankStatistics(dataFrame: DataFrame,
                          ranks: List[Long]): Map[Int, Iterable[Double]] = {
    require(ranks.forall(_ > 0))
    val numberOfColumns = dataFrame.schema.length

    val pairRDD: RDD[(Int, Double)] = mapToKeyValuePairs(dataFrame)
    pairRDD.foreach(x=> println(x))
    var i = 0
    var result = Map[Int, Iterable[Double]]()
    while (i < numberOfColumns) {
      //val col = dataFrame.rdd.flatMap(row => Range( 0,numberOfColumns).map(j => row.getDouble(j)))
      val col = dataFrame.rdd.map(row => row.getDouble(i))
      col.foreach(x=>(println(x)))
      val sortedCol: RDD[(Double, Long)] = col.sortBy(v => v).zipWithIndex()
      sortedCol.foreach(x=> println(x._1.toDouble +" " + x._2.toLong ))
      val ranksOnly = sortedCol.filter {
        //rank statistics are indexed from one. e.g. first element is 0
        case (colValue, index) => ranks.contains(index + 1)
      }.keys
      val list = ranksOnly.collect()
      result +=(i -> list)
      i += 1
    }
    result
  }


  def mapToKeyValuePairs(dataFrame: DataFrame): RDD[(Int, Double)] = {

    val rowLength = dataFrame.schema.length
    dataFrame.rdd.flatMap(
      row => Range(0, rowLength).map(i => (i, row.getDouble(i))) )
  }


}
