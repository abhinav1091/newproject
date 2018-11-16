package main.scala

class Sequence[T](steps: Seq[List[T]] ,steps2: Seq[List[T]]){

  def seq1():Unit={
    val x: Seq[List[Int]] = Seq(List(1, 2, 3, 4), List(2, 3, 4, 5))
    x.foreach(y=>{println(y)
                  })

  }
}

class Sequ[T](val i: T){
  def hello:Unit=
  println(i)
  }

object Sequence {
     def apply[T](steps: Seq[List[T]] ,steps2: List[T]*): Sequence[T] = {
        new Sequence(steps.toSeq ,steps2.toSeq)
       }
}

object Sequ{
  def apply[T](i : T) : Sequ[T] ={
   new Sequ(i)
  }
}