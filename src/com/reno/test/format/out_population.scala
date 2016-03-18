package com.reno.test.format

/**
 * Created by reno on 2015/10/20.
 */
object out_population extends Serializable{
  val mRand:java.util.Random=  new java.util.Random(System.nanoTime())

  /**
   *
   * @param datas
   * @param len
   * @return
   */
  def getDistinct(datas:Array[Int],len:Int):Array[Int]={
    var  flag:Boolean=true
    val out=Array.fill[Int](len){(-1)}
    for(i<-0 to len-1){
      while(flag){
        val enum=mRand.nextInt(datas.length)
        if(!out.contains(datas(enum))){
          out(i)=datas(enum)
          flag=false
        }
      }
      flag=true
    }
    out
  }

}
