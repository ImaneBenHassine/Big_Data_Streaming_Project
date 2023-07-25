package packageTest.PBGS

class ClassTest {

  def count_package (text : String) : Int ={
    if (text.isEmpty){
      0
    }else {
      text.trim.length
    }
  }

}
