/**
 * Created by reno on 2015/9/19.
 */
package com.reno.test.format

object SDPSO{

  def getFit(position:Array[Int]):Array[Float]={
    val a=position.length
    val out =Array[Float]()
    Array(0.5f,0.5f)
  }


  def main(args: Array[String]){
    println(args.length)
    args.map(x=>println(x+1))
    val one=Array(1,2,3)
    val two=Array(4,5,6)
    val test =new BaseParticle(one,two)
    test.speed.map(print(_))
    println("----")
    test.getFitness(getFit(_))
    test.ALL_Fitness.map(print(_))


  }


}