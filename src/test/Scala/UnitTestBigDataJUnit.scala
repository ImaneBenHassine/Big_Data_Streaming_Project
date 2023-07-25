import junit.framework.TestCase
import org.junit._
import org.junit.Assert._
class UnitTestBigDataJUnit   {

   @Test // Denotes that a method is a test method --> !!fundamental
 def testDivision () :Unit = {

     var value_actual : Double = HelloScala.division(10,2)
     var value_expect : Int = 5
     assertEquals("the result of this division must be 5", value_expect,value_actual.toInt )
 }

  @Test
  def testConversion () : Unit = {

    var value_actual : Int = HelloScala.convert_int("17")
    var value_expect : Int = 17
    assertSame("the result of this conversion must a number ", value_expect, value_actual)
  }
}
