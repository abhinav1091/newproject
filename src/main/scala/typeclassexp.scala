package scala

import scala.reflect.ClassTag

object typeclassexp {

  def extract[T](list: List[Any])(implicit tag: ClassTag[T]) =
    list.flatMap {
      case element: T => Some(element)
      case _ => None
    }

  def main (args : Array[String]): Unit = {
    val list = List(1, "string1", List(), "string2")
    val result = extract[String](list)
    println(result) // List(1, string1, List(), string2)
  }
}
