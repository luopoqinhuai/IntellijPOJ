package renoformat.start

import org.apache.spark.graphx.{Graph, GraphLoader}
import org.apache.spark.{SparkConf, SparkContext}
import renoformat.{DDPSO_rdd_3, gen}

/**
 * Created by reno on 2015/12/5.
 */
object DDPSO {
  def doOneNet(sc:SparkContext,fpath:String,vertexNum:Int,outPath:String)={
    val Tgraph:Graph[gen,Int] = GraphLoader.edgeListFile(sc, fpath,numEdgePartitions = 200).mapVertices[gen]{(x,y)=> new gen()}.cache()
    val Tpopulation = new DDPSO_rdd_3(Tgraph, vertexNum)
    val Tparticles = Tpopulation.init(sc,100,200,0.3)
    val Toutt=Tpopulation.run(Tparticles,800)
    Toutt.saveAsObjectFile(outPath)

    val bestone=Toutt.map(x=>(x._1,x._2.Pg_p(0)))
    val ng=Tgraph.mapVertices((vid,g)=>1)
    println(fitnessQ.Fitness_Q(bestone,ng,vertexNum))
    Tgraph.unpersistVertices()
    Tgraph.edges.unpersist()
    Toutt.unpersist()

  }

  /*
    val fpath1 = "hdfs://master:54321/home/spark/community_detection/Edgedolphin.txt"
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
    val vertexNum7=317080
    */
  def main(args: Array[String]) {
    System.setProperty("spark.driver.memory", "20G")
    System.setProperty("spark.akka.frameSize", "1024")
    System.setProperty("spark.storage.memoryFraction", "0.5")
    val mname = "DDPSO_new" + System.nanoTime()
    val conf = new SparkConf().setAppName(mname)
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("/home/spark/checkpoint")
    val fpath8="hdfs://master:54321/home/spark/community_detection/EdgeEmail-Enron.txt"
    val vertexNum8=36692
    doOneNet(sc,fpath8,vertexNum8,"/home/spark/A_RENO_email_"+System.nanoTime+".rdds")

    val Tgraph = GraphLoader.edgeListFile(sc, fpath8,numEdgePartitions = 200);
    val res=sc.objectFile[(Long,gen)]("/home/spark/A_RENO_email_2893507692488706.rdds")
    val clu=res.map(x=>(x._1,x._2.Pg_p(0)))
    fitnessQ.Fitness_Q(clu,Tgraph,36692)
   // val Tgraph:Graph[gen,Int] = GraphLoader.edgeListFile(sc, fpath8,numEdgePartitions = 200).mapVertices[gen]{(x,y)=> new gen()}.cache()
  }

}
