package com.reno.main
/**
 * Created by reno on 2015/10/12.
 */
object testmain {

  def main(args: Array[String]): Unit ={
    /*val one =new pase_Particle(Array(1,2,3,4,7,8,9,10))
    val two =new pase_Particle(Array(2,3,4,5,8,9,0,10))
    val out=one.cross(two)
    println(out._1)
    println(out._2)*/
    val rand:java.util.Random=  new java.util.Random(System.nanoTime())
    def getDistinct(datas:Array[Int],len:Int):Array[Int]={
      //scala.collection.mutable.ArraySeq
      var  flag:Boolean=true
      val out=Array.fill[Int](len){(-1)}
      for(i<-0 to len-1){
        while(flag){
          val enum=rand.nextInt(datas.length)
          if(!out.contains(datas(enum))){
            out(i)=datas(enum)
            flag=false
          }
        }
        flag=true
        println("-----"+ out(i))
      }
      out
    }
println("start")
    val out=getDistinct(Array(1,2,3,4,5,6,7,8,9,0),3)
    println("end")
    print (out(0)+" "+out(1)+" "+out(2))


  }
}
