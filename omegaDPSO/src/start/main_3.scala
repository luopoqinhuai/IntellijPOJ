package start

import format.{DDPSO_rdd_3, gen}
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
    val Tparticles = Tpopulation.init(sc,10,1000,0.3)
    val Toutt=Tpopulation.run(Tparticles,800)
    Toutt.saveAsObjectFile(outPath)
    Tgraph.unpersistVertices()
    Tgraph.edges.unpersist()
    Toutt.unpersist()
    Tgraph.triangleCount().vertices

  }

  def main(args: Array[String]) {
    System.setProperty("spark.driver.memory", "60G")
    System.setProperty("spark.akka.frameSize", "1024")
    System.setProperty("spark.storage.memoryFraction", "0.4")

    val mname = "new DPSO" + System.nanoTime()
    val conf = new SparkConf().setAppName(mname)
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("/home/spark/checkpoint")


    val fpath="hdfs://master:54321/home/spark/community_detection/EdgeEmail-Enron.txt"
    val vertexNum=36692

    val Tgraph:Graph[gen,Int] = GraphLoader.edgeListFile(sc, fpath,numEdgePartitions = 200).mapVertices[gen]{(x,y)=> new gen()}.cache()
    val Tpopulation = new DDPSO_rdd_3(Tgraph, vertexNum)
    val Tparticles = Tpopulation.init(sc,50,1000,0.3)


    val a=System.currentTimeMillis()
    Tpopulation.testFitness(Tparticles)
    val b=System.currentTimeMillis()
    print (b-a)





  }

}
