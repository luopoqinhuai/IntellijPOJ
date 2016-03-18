package com.reno.communicity.format



/**
 * Created by reno on 2015/11/23.
 */

import org.apache.spark.rdd.RDD

class rdd_particle2(var positionAndSpeed:RDD[(Long,(Int,Int))]) {
  var Pg_p:RDD[(Long,Int)]=positionAndSpeed.map(x=>(x._1,x._2._1))
  var Pp_p:RDD[(Long,Int)]=positionAndSpeed.map(x=>(x._1,x._2._1))
  var gFitness:Double=100000.0
  var pFitness:Double=100000.0
  var Fitness :Double=100000.0
  var Nbest:RDD[(Long,Int)]=null


  def repartition(): Unit ={
    this.Pg_p=Pg_p.repartition(40)
    this.Pp_p=Pp_p.repartition(40)
    this.positionAndSpeed=positionAndSpeed.repartition(40)
    this.Nbest=Nbest.repartition(40)
  }
  def checkpoint(): Unit ={
    this.Pg_p.checkpoint()
    this.Pp_p.checkpoint()
    this.positionAndSpeed.checkpoint()
    this.Nbest.checkpoint()
  }
}
