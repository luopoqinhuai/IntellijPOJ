package com.reno.recommecdation.tools
/**
 * Created by reno on 2015/11/2.
 */

object formatTools{
  def ArrToStr(arr:Array[Int]):String={
    var s=""
    for(i<-arr){
      s+=i+" "
    }
    s
  }


  def checkrepeate(arr:Array[Int]):Int={
    val len1=arr.length
    val len2=arr.toSet.size
    if(len1==len2)   0
    else             1

  }
}
