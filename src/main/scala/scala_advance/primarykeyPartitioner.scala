package scala_advance
import org.apache.spark.{HashPartitioner, Partitioner}

class primarykeyPartitioner[K,S] (partitions:Int) extends Partitioner {

  val delegatePartioner = new HashPartitioner (numPartitions)

  override def numPartitions = delegatePartioner.numPartitions

  override def getPartition(key: Any): Int = {
return 3
  }
}
