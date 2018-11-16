package scala_advance
import scala.reflect.ClassTag
import scala.math.Ordering

object ScalaType {

  def extract[T](list: List[Any])(implicit tag: ClassTag[T]) =
    list.flatMap {
      case element: T => Some(element)
      case _ => None
    }


  def main(args:Array[String]):Unit= {

    val list: List[Any] = List(1, "string1", List(), "string2", 2, 3)
    val result = extract[Int](list)
    println(result) // List(string1, string2)

    /*  // implicit val emp: ClassTag[Int]
var list1: List[Any]=List(1, "string1", List(), "string2",2,3)
   val x= list1.flatMap
    {
      case element: Int  => Some(element)
      case _ => None
    }
    */
  }


  def order[T: Ordering](t: Array[T]): Unit = {
    val ordering = implicitly[Ordering[T]]

    // computing h value
    var h = 1
    while (h <= t.length / 9) {
      h = 3 * h + 1
    }

    while (h > 0) {
      for (i <- h until t.length) {
        // Sorting each a h-zone
        move_element_at_index_to_sorted_position_on_the_left(t,
          i,
          ordering,
          step = h)
      }
      h /= 3
    }

  }


  def move_element_at_index_to_sorted_position_on_the_left[T](
                                                               t: Array[T],
                                                               i: Int,
                                                               ordering: Ordering[T],
                                                               step: Int = 1): Unit = {
    val v = t(i)
    var j = i
    while (j >= step && ordering.gt(t(j - step), v)) {
      t(j) = t(j - step)
      j -= step
    }
    t(j) = v
  }


}

