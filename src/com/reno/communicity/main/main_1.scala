package com.reno.communicity.main

import java.io.{PrintWriter, FileOutputStream, ObjectOutputStream}

import com.reno.communicity.format.DDPSO
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by reno on 2015/11/23.
 */
object main_1 {

  def main(args: Array[String]) {
    System.setProperty("spark.driver.memory","40G")
    System.setProperty("spark.akka.frameSize","1024")
    System.setProperty("spark.storage.memoryFraction","0.5")
    val mname="DDPSO"+System.nanoTime()
    val conf = new SparkConf().setAppName(mname)
    val sc = new SparkContext(conf)
    //val fpath = "hdfs://master:54321/home/spark/community_detection/Edgedolphin.txt"
    val fpath=args(0)
    val vertexNum=args(1).toInt
    val graph =GraphLoader.edgeListFile(sc, fpath)
    val population=new DDPSO(graph,vertexNum)
    val one=sc.makeRDD(1 to 50)
    val mparticles=population.init(one)

    println("\n\n\n\n"++"init over"+"\n\n\n\n")

    val out=population.run(mparticles,20)
    val suiyione=out.first
    val Gbest=suiyione.Pg_p
    val outObj=new ObjectOutputStream(new FileOutputStream("/home/spark/reno/"+mname+".obj"))
    outObj.writeObject(Gbest)
    outObj.close()
    println("\n\n\n"+suiyione.gifitness+" "+suiyione.fitness+" "+suiyione.pifitness+"\n\n\n")

  }





}
