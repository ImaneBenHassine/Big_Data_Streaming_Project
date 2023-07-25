// model test is FunSuite
import org.scalatest
import org.scalatest.funsuite.AnyFunSuite
class UnitTestBigDataScalaTest extends AnyFunSuite {

  test("the division must return 10") {
    assert(HelloScala.division(20,2) === 10)
  }

  test("the division must return an error type ArithmeticException ") {
    assertThrows[ArithmeticException](
      assert(HelloScala.division(20,0) === 10)
    )
  }

}
