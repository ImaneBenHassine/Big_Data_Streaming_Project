// Using Suite FlatSpecs

import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest._
class FlatSpecTests extends AnyFlatSpec with Matchers {
  "The division result" should "be 10" in {
    assert(HelloScala.division(20, 2) === 10)
  }

  "an ArithmeticException when division is invoked" should "be thrown" in {
    an [ArithmeticException] should be thrownBy (HelloScala.division(20, 0))
  }

  it should "send an OutofBound Error" in {
    var list_fruits : List[String] = List ("orange","kiwi","banana")
    assertThrows[IndexOutOfBoundsException](list_fruits(4))

  }
  it should("returns the starting letters of the string") in {
    var text : String = "it is funny working with ScalaTest"
    text should startWith("i")
  }
}

