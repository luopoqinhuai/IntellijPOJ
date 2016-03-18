package com.reno.communicity.test

/**
 * Created by reno on 2016/1/7.
 */
object testtt {

  def main(args: Array[String]) {

    def run1000(): Unit ={
      var i=0
      while(i<1000){
        i+=1
      }
    }

    val startTime=System.currentTimeMillis()
    val aaa=100
    var i=0
    while(i<aaa){
      print(i)
      run1000()
      i+=1
    }
    print("\n")


    val endTime=System.currentTimeMillis()

    print("ssss"+(endTime-startTime))
  }







}
