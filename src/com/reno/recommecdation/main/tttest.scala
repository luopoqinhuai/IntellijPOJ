package com.reno.recommecdation.main


import com.reno.recommecdation.GA1
import com.reno.recommecdation.format.reno_particle
import com.reno.recommecdation.tools.formatTools

/**
 * Created by reno on 2015/10/21.
 */




object tttest {
  def returnDist(ll:List[reno_particle]):List[reno_particle]={
    val all=ll.toArray
    println(all.length)
    var distOut=List.empty[reno_particle]
    for(i<-all){
      println(i.position(0)+" "+i.position(1)+" "+i.position(2)+" "+i.position(3)+" ")
      if(!(i in distOut.toArray)){
        distOut=i::distOut
      }
    }
    distOut
  }
  def main(args: Array[String]): Unit = {
    val a1=new reno_particle(Array(1,4,3,5,6))
    val a2=new reno_particle(Array(1,3,2,4,5))
    val out=GA1.cross(a1,a2)
    val na=out(0)
    val nb=out(1)
    println(formatTools.ArrToStr(na.position))
    println(formatTools.ArrToStr(nb.position))


  }

}
