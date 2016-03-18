package com.reno.main

import com.reno.format.reno_particle

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
 /*   val a1=new reno_particle(Array(1,2,3,4))
    val a2=new reno_particle(Array(1,2,6,4))
    val a3=new reno_particle(Array(1,2,7,4))
    val a4=new reno_particle(Array(1,2,8,4))
    val a5=new reno_particle(Array(1,2,3,4))
    val arrs=Array(a1,a2,a3,a4,a5)
    val ooo= returnDist(arrs.toList)*/
    println(123)

  }

}
