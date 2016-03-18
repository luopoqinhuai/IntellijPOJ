package com.reno.communicity.format

/**
 * Created by reno on 2015/10/28.
 */
class cm_particle (val position:Array[(Long,Int)],val speed:Array[Int]) extends  Serializable{
  var fitness:Double=1000000
  var pifitness:Double=1000000
  var gifitness:Double=1000000
  var Pg_p=this.position
  var Pi_p=this.position
  private val len=position.length
  var Nbest=Array.fill(len){-1}
}
