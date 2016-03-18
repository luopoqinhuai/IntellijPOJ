package com.reno.communicity.main

import org.apache.spark.graphx.{GraphLoader, Graph}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by reno on 2015/12/22.
 */
class initialBeforedo {
  System.setProperty("spark.driver.memory", "60G")
  System.setProperty("spark.akka.frameSize", "1024")
  System.setProperty("spark.storage.memoryFraction", "0.4")

  val mname = "DDPSO" + System.nanoTime()
  val conf = new SparkConf().setAppName(mname)
  val sc = new SparkContext(conf)
  sc.setCheckpointDir("/home/spark/checkpoint")

  val fpath= "/home/spark/community_detection/EdgeEmail-Enron.txt"

  val mGraph:Graph[Int,Int] = GraphLoader.edgeListFile(sc, fpath,numEdgePartitions = 2000).cache()
  val mDegrees= mGraph.degrees.cache()



}
