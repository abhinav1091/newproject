package scala_advance

object lmplicit_example {

  def getIndex[T, CC](seq: CC, value: T)(implicit conv: CC => Seq[T]) = seq.indexOf(value)

  def main( args:Array[String]):Unit= {
    println(getIndex("abc",'c'))
  }
}
