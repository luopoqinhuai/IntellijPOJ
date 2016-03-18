package com.reno.communicity.main


import com.reno.communicity.format.{DDPSO_rdd_3, gen}
import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by reno on 2015/12/5.
 */
object main_3 {


  // val mparticles = sc.objectFile[(Long,gen)]("/home/spark/store50_test_football.rdds")
  def doOneNet(sc:SparkContext,fpath:String,vertexNum:Int,outPath:String)={
    val Tgraph:Graph[gen,Int] = GraphLoader.edgeListFile(sc, fpath,numEdgePartitions = 200).mapVertices[gen]{(x,y)=> new gen()}.cache()
    val Tpopulation = new DDPSO_rdd_3(Tgraph, vertexNum)
    val Tparticles = Tpopulation.init(sc,10,200,0.3)
    val Toutt=Tpopulation.run(Tparticles,50)
    Toutt.saveAsObjectFile(outPath)
    Tgraph.unpersistVertices()
    Tgraph.edges.unpersist()
    Toutt.unpersist()
  }

  def main(args: Array[String]) {
    System.setProperty("spark.driver.memory", "20G")
    System.setProperty("spark.akka.frameSize", "1024")
    System.setProperty("spark.storage.memoryFraction", "0.5")

    val mname = "DDPSO_rdd" + System.nanoTime()
    val conf = new SparkConf().setAppName(mname)
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("/home/spark/checkpoint")

/*    val fpath1 = "hdfs://master:54321/home/spark/community_detection/Edgedolphin.txt"
    val vertexNum1 =62
    val fpath2="/home/spark/community_detection/EdgeSFI.txt"
    val vertexNum2=118
    val fpath3="/home/spark/community_detection/Edgedata_Journal.txt"
    val vertexNum3=40
    val fpath4="/home/spark/community_detection/Edgekarate.txt"
    val vertexNum4=34

    val fpath5="/home/spark/community_detection/Edgefootball.txt"
    val vertexNum5=115

    val fpath6="/home/spark/community_detection/Edgenetscience_remove.txt"
    val vertexNum6=1589



    val fpath7 = "hdfs://master:54321/home/spark/community_detection/dblp.ungraph.edge.txt"
    val vertexNum7=317080*/

    val fpath8="hdfs://master:54321/home/spark/community_detection/EdgeEmail-Enron.txt"
    val vertexNum8=36692
    doOneNet(sc,fpath8,vertexNum8,"/home/spark/store50_email_2016_50"+System.nanoTime+".rdds")

   // val mparticles = sc.objectFile[(Long,gen)]("/home/spark/store50_test_Big.rdds")
/*    val output=new PrintWriter("/home/spark/reno/out_science_remove.txt")
    val mparticles = sc.objectFile[(Long,gen)]("/home/spark/store50_test_science_remove.rdds")
    mparticles.map(x=>(x._1,x._2.Pg_p(0))).collect.foreach(x=>output.write(x.toString()+"\n"))
    output.close()*/


   // val test=sc.makeRDD(1 to 100).map(x=>(x,x))
   // val chlq=sc.makeRDD(1 to 9,3).mapPartitionsWithIndex((x,y)=>if(x==2) y else y.map(j=>j*2))












  }

}
