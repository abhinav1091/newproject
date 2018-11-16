package main.scala
import org.apache.spark.Partitioner
class partitioner_custom (override val numPartitions: Int) extends Partitioner {

  def getPartition(key: Any): Int = key match {
    case s: String => {
      if (s(0).toUpper > 'J') 1 else 0
    }
  }
}



