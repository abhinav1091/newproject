package join

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag

object broadcast_join {

  def manualBroadCastHashJoin[K : Ordering : ClassTag, V1 : ClassTag, V2 : ClassTag](bigRDD : RDD[(K, V1)],
                                                                                     smallRDD : RDD[(K, V2)]):RDD[(K,(V1,V2))]= {
    val smallRDDLocal: scala.collection.Map[K, V2] = smallRDD.collectAsMap()
    bigRDD.sparkContext.broadcast(smallRDDLocal)

    bigRDD.mapPartitions(iter => {
      iter.flatMap {
        case (k:K,v1:V1 ) =>
          smallRDDLocal.get(k) match {
        case None => Seq.empty[(K, (V1, V2))]
        case Some(v2:V2) => Seq((k, (v1, v2))) }
    }
    }, preservesPartitioning = true)
  }
  //end:coreBroadCast[]

  def main(args:Array[String]):Unit={

    val session= SparkSession.builder().appName("broadcast join").master("local[2]").getOrCreate()

    val bigrdd = session.read.format("csv").option("header", "true").
      option("inferschema", value = true).load("/Users/abhinavkumar/Downloads/2017-06/2017-06-metropolitan-street.csv")
      .select("Crime ID", "Falls within").rdd.map(x=> (x(1), x(2)))

    val smallrdd = session.read.format("csv").option("header", "true").option("inferschema", value = true).load("/Users/abhinavkumar/Downloads/2017-06/2017-06-metropolitan-street.csv")
      .select("Crime ID", "Falls within").rdd.map(x=> (x(1), x(2)))

    val res= manualBroadCastHashJoin(bigrdd, smallrdd)

   res.foreach( str=> _ match { case   (k, (v1, v2))  => print(k)})
  }

}
