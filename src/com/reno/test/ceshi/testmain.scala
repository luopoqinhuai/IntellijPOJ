/**
 * Created by reno on 2015/11/5.
 */
import java.io._

class Student(val name: String, val sex: Short, val addr: String) extends Serializable {
  override def toString(): String = {
    "name:%s,sex:%d,addr:%s".format(name, sex, addr)
  }
}


object main {

  def main(args: Array[String]) {
    //val studentA = new Student("张三", 2, "广州")
    //val studentB = new Student("李四", 2, "广州")
    //val outObj = new ObjectOutputStream(new FileOutputStream("E:/datas/student.obj"))
    //outObj.writeObject(studentA)
    //outObj.close()
    val in = new ObjectInputStream(new FileInputStream("E:/datas/student.obj"))
    println(in.readObject().asInstanceOf[Student])
    print("over")



  }
}