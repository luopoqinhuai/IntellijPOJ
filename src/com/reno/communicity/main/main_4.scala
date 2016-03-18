package com.reno.communicity.main

import org.apache.spark.graphx.{TripletFields, Graph, GraphLoader}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by reno on 2015/12/19.
 */
object main_4 {

  def combine(A:(Long,Int),B:(Long,Int)):(Long,Int)={
    if(A._2>B._2) A
    if (A._2<B._2) B
    else{
      val mRand: java.util.Random = new java.util.Random(System.nanoTime())
      if(mRand.nextDouble()>0.5) A
      else B
    }
  }

  def main(args: Array[String]) {
    System.setProperty("spark.driver.memory", "60G")
    System.setProperty("spark.akka.frameSize", "1024")
    System.setProperty("spark.storage.memoryFraction", "0.4")
    val mname = "DDPSO" + System.nanoTime()
    val conf = new SparkConf().setAppName(mname)
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("/home/spark/checkpoint")


    //val fpath="hdfs://master:54321/home/spark/community_detection/dblp.ungraph.edge.txt"
    //val fpath= "hdfs://master:54321/home/spark/community_detection/EdgeCA-AstroPh.txt"
    //val fpath= "/home/spark/community_detection/EdgeCA-CondMat.txt"

   // val fpath= "/home/spark/community_detection/EdgeCA-AstroPh.txt"



    val fpath= "/home/spark/community_detection/EdgeEmail-Enron.txt"

    //val fpath=  "/home/spark/community_detection/EdgeCA-GrQc.txt"

    //val fpath=  "/home/spark/community_detection/EdgeCA-HepPh.txt"

    //val fpath="/home/spark/community_detection/EdgeCA-HepTh.txt"
    val mGraph:Graph[Int,Int] = GraphLoader.edgeListFile(sc, fpath,numEdgePartitions = 2000).cache()
    val mDegrees= mGraph.degrees.cache()

    val verticlenum=mDegrees.count
    val edgenum=mGraph.edges.count

    val two=mDegrees.filter(x=>x._2==2).count
    val l6=mDegrees.filter(x=>x._2<6).count
    val l12=mDegrees.filter(x=>x._2<12).count
    val l24=mDegrees.filter(x=>x._2<24).count
    val l48=mDegrees.filter(x=>x._2<48).count
    val l96=mDegrees.filter(x=>x._2<96).count
    val b96=mDegrees.filter(x=>x._2>=96).count

    println(verticlenum)
    println(edgenum)
    println(two)
    println(l6)
    println(l12)
    println(l24)
    println(l48)
    println(l96)
    println(b96)

    mDegrees.unpersist()
    mGraph.unpersist()













    val dGraph= mGraph.joinVertices(mDegrees){(vid,vdata ,U)=>U}.cache()
    dGraph.edges.count()

    val init1=dGraph.aggregateMessages[(Long,Int)](
      triplet => {
        val scrID:Long=triplet.srcId
        val scrDegree:Int=triplet.srcAttr
        if(triplet.srcAttr<triplet.dstAttr){
          triplet.sendToDst((scrID,scrDegree))
        }else{
          triplet.sendToDst((scrID,-1))
        }
      },(A,B)=>combine(A,B),
      TripletFields.All)


  }

}
