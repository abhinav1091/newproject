package scala

import main.scala.cube
import main.scala.cubecalculator
import collection.mutable.Stack
import org.scalatest.{FlatSpec,Ignore,PrivateMethodTester}
//import org.scalatest.junit.JUnitRunner
import org.scalatest._

//@RunWith(classOf[JUnitRunner])
class cubetest  extends FlatSpec  {


  "cube" should "run" in {


    val x = cube.cube(4)
    assert(cube.cube(4) === 64)
 }


   "cubeclass" should "run" in {
     val y = new cubecalculator
     assert(y.cube(5) === 125)
   }


  "TestWithEmptySpace" should " not run" in {
    val expectedValue = true
    val actualValue = true
    assert(expectedValue === actualValue)

  }

  def runTest(): Unit = {
    val expectedValue = true
    val actualValue = true
    assert(expectedValue === actualValue)
  }


}
