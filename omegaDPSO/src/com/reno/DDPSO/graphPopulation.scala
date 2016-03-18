package com.reno.DDPSO

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, GraphLoader, VertexRDD}

/**
 * Created by reno on 2016/1/12.
 */
class graphPopulation(sc:SparkContext,graphPath:String,popNum:Int,vertexNum:Int) extends  Serializable {
  val graphPopulation:Array[graphParticle]=new Array(popNum)
  var gBest:VertexRDD[Int]=null
  var rootGraph:Graph[Int,Int]=GraphLoader.edgeListFile(sc,graphPath)

  def initPopulation(): Unit ={
    val mRand: java.util.Random = new java.util.Random(System.nanoTime())
    for(popu<-0 to graphPopulation.length-1){
      graphPopulation(popu)=new graphParticle()
      val tmp=graphPopulation(popu)
      tmp.netstate=rootGraph.mapVertices((vid,arr)=>mRand.nextInt(vertexNum))//here can add some heuristic info
      tmp.netstate.cache()
      tmp.netstate.vertices.count()
      tmp.netstate.edges.count()
      tmp.pBest=tmp.netstate.vertices.mapValues(x=>x)
      tmp.pBest.cache()
      tmp.pBest.count()
    }
  }
}

object ceshi{
  val sc = new SparkContext()
  val pop=new graphPopulation(sc,"/home/spark/community_detection/Edgedolphin.txt",10,62)
  pop.initPopulation()
}
