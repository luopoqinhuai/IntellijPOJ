package com.reno.DDPSO

import org.apache.spark.graphx.{VertexRDD, Graph}



/**
 * Created by reno on 2016/1/12.
 */
class graphParticle extends  Serializable{
  var netstate:Graph[Int,Int]=null
  var pBest:VertexRDD[Int]=null
  var graphFitness:Double=Double.MaxValue
}
