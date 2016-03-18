package com.reno.communicity.main

import com.reno.communicity.format.DDPSO_rdd
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by reno on 2015/11/23.
 */
object main_2 {
  def main(args: Array[String]) {
    System.setProperty("spark.driver.memory", "60G")
    System.setProperty("spark.akka.frameSize", "1024")
    System.setProperty("spark.storage.memoryFraction", "0.4")

    val mname = "DDPSO_rdd" + System.nanoTime()
    val conf = new SparkConf().setAppName(mname)
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("/home/spark/checkpoint")
    val fpath = "hdfs://master:54321/home/spark/community_detection/Edgedolphin.txt"
    val vertexNum =62
    //val fpath = "hdfs://master:54321/home/spark/community_detection/dblp.ungraph.edge.txt"
    //val vertexNum=317080
    val graph:Graph[Int,Int] = GraphLoader.edgeListFile(sc, fpath,numEdgePartitions = 200).cache()
    //val totaledges=graph.edges.count()

    val population = new DDPSO_rdd(graph, vertexNum)
    val iter:Int=args(0).toInt
    val mparticles = population.init(sc,50)
    val outt=population.run(mparticles,1,iter)
    val bsttt:Array[(Long,Int)]=outt(0).Pg_p.collect()
    println("\n\n\n"+outt(0).gFitness+"\n\n\n")
    outt(0).Pg_p.saveAsObjectFile("/home/spark/Pg_p_"+ System.nanoTime()+".rdd")
  }

}
