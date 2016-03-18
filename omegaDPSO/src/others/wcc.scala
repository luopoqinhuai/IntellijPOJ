package others

/**
 * Created by reno on 2016/1/9.
 */
import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import scala.io.Source


object wcc {
  private[wcc] type tInS_ = Int
  private[wcc] type tInV_ = Int
  private[wcc] type vt_ = Int
  private[wcc] type vtNotS_ = Int
  private[wcc] type Msg = (tInS_,tInV_,vt_,vtNotS_)


  /**
   * USE THIS
   * @param networkPath
   * @param filenames
   * @param filepath
   * @param sc
   */
  def cacuWCC(networkPath:String,filenames:Array[String],filepath:String,sc:SparkContext): Unit ={
    val mgraph= GraphLoader.edgeListFile(sc,networkPath,numEdgePartitions = 200).cache()
    for(p<-filenames){
      wcc.renoWCC(sc,filepath,p,mgraph)
    }
  }



  def getWCCfromCluster(mcluster:Array[Int],network:Graph[Int,Int],sc:SparkContext): Double ={
    val c1 = sc.makeRDD(mcluster.zipWithIndex).groupBy(x=>x._1).flatMap { x =>
      val clu = x._2.toArray
      clu.map { case (a, b) => (b.toLong+1, (a, clu.length)) }
    }
      val netSize=network.vertices.count()
      val graphT:Graph[(Int,Int),Int] =network.outerJoinVertices[(Int,Int),(Int,Int)](c1)((vid,vd,U)=>U.getOrElse((0,0))).groupEdges((a, b) => a).cache()
      // clu Id
      WCC(graphT,netSize)
    }

  val rootpath="/home/spark/reno/tocacuWCC/"
  val netrootpath="/home/spark/community_detection/"
  val SFIpath=Array("GA_reno_SFI.txt", "memetic_reno_SFI.txt", "MOEAD_reno_SFI.txt", "NNIA_reno_SFI.txt", "NSGAII_reno_SFI.txt")
  val networkinfo="EdgeSFI.txt"

  val sc=new SparkContext()
  cacuWCC(netrootpath+networkinfo,SFIpath,rootpath,sc)






  val alldatas=Array("Edgedolphin.txt","Edgepolbooks.txt","Edgekarate.txt","Edgedata_Journal.txt","Edgebanchmart_connect.txt","Edgefootball.txt")
  val twodatas=Array("SFI.txt","netscience_remove.txt")


  val dolphin_path=Array("nsgaii_dolphin.txt","NNIA_dolphin.txt","moead_dolphin.txt","memetic_dolphin.txt", "ga-net_dolphin.txt")
  val mgraph= GraphLoader.edgeListFile(sc,netrootpath+ alldatas(0),numEdgePartitions = 200).cache()

  for(p<-dolphin_path){
    wcc.renoWCC(sc,rootpath,p,mgraph)
  }


  val polbooks_path=Array( "nsgaii_polbooks.txt","NNIA_polbooks.txt","moead_polbooks.txt", "memetic_polbooks.txt", "ga-net_polbooks.txt")
  val mgraphpol= GraphLoader.edgeListFile(sc,netrootpath+ alldatas(1),numEdgePartitions = 200).cache()

  for(p<-polbooks_path){
    wcc.renoWCC(sc,rootpath,p,mgraphpol)
  }



  val Joural_path=Array("nsgaii_Joural.txt","NNIA_Joural.txt","moead_Joural.txt","memetic_Joural.txt","ga-net_Joural.txt")





  val alllll=Array(football_path,Joural_path,karate_path)

  val alldatas2=Array("Edgekarate.txt","Edgedata_Journal.txt","Edgefootball.txt")

for(i<-0 to 2){
  val mgraphtmp2= GraphLoader.edgeListFile(sc,netrootpath+ alldatas2(i),numEdgePartitions = 200).cache()
  for(p<-alllll(i)){
    wcc.renoWCC(sc,rootpath,p,mgraphtmp2)
  }
}


  val alldatas3=Array("EdgeSFI.txt","Edgenetscience_remove.txt")
  val SFI_path=Array("GA_reno_SFI.txt","memetic_reno_SFI.txt", "MOEAD_reno_SFI.txt", "NNIA_reno_SFI.txt", "NSGAII_reno_SFI.txt")

  val alllll2=Array(SFI_path,Science_path)



  val mgraphfootball= GraphLoader.edgeListFile(sc,netrootpath+ "Edgefootball.txt",numEdgePartitions = 200).cache()
  val football_path=Array("nsgaii_football.txt","NNIA_football.txt","moead_football.txt", "memetic_football.txt","ga-net_football.txt")
  for(p<-football_path) {
    wcc.renoWCC(sc, rootpath, p, mgraphfootball)
  }

  val karate_path=Array("nsgaii_karate.txt", "NNIA_karate.txt", "moead_karate.txt", "memetic_karate.txt","ga-net_karate.txt")
  val mgraphkarat= GraphLoader.edgeListFile(sc,netrootpath+ "Edgekarate.txt",numEdgePartitions = 200).cache()
  for(p<-karate_path) {
    wcc.renoWCC(sc, rootpath, p, mgraphkarat)
  }


  val Science_path=Array("GA_reno_netscience.txt","MOEAD_reno_netscience.txt", "NNIA_reno_netscience.txt","NSGAII_reno_netscience.txt")
  val mgraphscience= GraphLoader.edgeListFile(sc,netrootpath+ "Edgenetscience_remove.txt",numEdgePartitions = 200).cache()
  for(p<-Science_path) {
    wcc.renoWCC(sc, rootpath, p, mgraphscience)
  }


  val mgraphca_qc= GraphLoader.edgeListFile(sc,netrootpath+ "EdgeCA-GrQc.txt",numEdgePartitions = 200).cache()


   wcc.renoWCC(sc, rootpath, "nnia_ca.txt", mgraphca_qc)

  wcc.renoWCC(sc, rootpath, "ga_ca.txt", mgraphca_qc)

  wcc.renoWCC(sc, rootpath, "nsgaii_ca.txt", mgraphca_qc)









    val mgraphSFI= GraphLoader.edgeListFile(sc,netrootpath+networkinfo,numEdgePartitions = 200).cache()
    for(p<-otnerSFI){
      wcc.renoWCC(sc,rootpath,p,mgraphSFI)
    }













  val otnerSFI=Array(
    "GA_reno_SFI.txt",
    "memetic_reno_SFI.txt",
    "MOEAD_reno_SFI.txt",
    "NNIA_reno_SFI.txt",
    "NSGAII_reno_SFI.txt")
  val otherScience=Array("GA_reno_netscience.txt","MOEAD_reno_netscience.txt", "NNIA_reno_netscience.txt","NSGAII_reno_netscience.txt")
















  def renoWCC(sc:SparkContext,rootpath:String,fpath:String,network:Graph[Int,Int])={
    network.subgraph(a=>true,(vid,d)=>vid!=1)
      val mOutdatas=Source.fromFile(rootpath+fpath).getLines()
      val write=new PrintWriter(new File(rootpath+fpath+"_wcc.txt"))
      for(particle<-mOutdatas){
        val one=particle.stripLineEnd.split("\t").map(x=>x.toInt)
        val mWcc=getWCCfromCluster(one,network,sc)
        write.write(mWcc+"\r\n")
      }
      write.close()
  }






  def GetWcc(name:String,sc:SparkContext): Unit ={
    val fpath ="/home/spark/community_detection/"
    val fcmtys = "/home/spark/chlq/output"+name+".txt"

    val output=new PrintWriter("/home/spark/chlq/WCC_"+name+".txt")

    val graph=GraphLoader.edgeListFile(sc, fpath+name+".ungraph.edge.txt")
    val netSize=graph.vertices.count()
    val c1All=scala.io.Source.fromFile(fcmtys).getLines()
    var cc=0
    while(c1All.hasNext) {
      cc = cc + 1
      val c1Temp = c1All.next().toString.split("::")(1).split("\t").map(x => x.toInt)
      val c1Size = c1Temp.length

      val c1 = sc.makeRDD(c1Temp.zipWithIndex).groupBy(x=>x._1).flatMap{x=>
        val clu=x._2.toArray
        clu.map{case (a,b) => (b.toLong,(a,clu.length))}
      }
      // clu Id,num of clu
      val graphT:Graph[(Int,Int),Int] =graph.outerJoinVertices[(Int,Int),(Int,Int)](c1)((vid,vd,U)=>U.getOrElse((0,0))).groupEdges((a, b) => a).cache()
      // clu Id
      val wcc_ : Double = WCC(graphT,netSize)

      output.println(cc+"\t"+wcc_)
    }
    output.close()
  }



  def WCC(graphT:Graph[(Int,Int),Int],netSize:Long):Double={
    val nbrSets: VertexRDD[Set[(Long,Int)]] =
      graphT.aggregateMessages[Set[(VertexId, Int)]](
        ctx => ctx.sendToSrc(Set((ctx.dstId, ctx.dstAttr._1))),
        (a, b) => a ++ b
      )
    //(clu Id,num of clu),Set[(Id,clu Id)]
    val setGraph: Graph[((Int,Int),Set[(Long,Int)]), Int] = graphT.outerJoinVertices(nbrSets) {
      (vid, clu, optSet) => (clu,optSet.getOrElse(null))
    }

    val trangleInfo:VertexRDD[(Int,Int,Int,Int)]  =
      setGraph.aggregateMessages[(Int,Int,Set[Long],Set[Long])](edgeFunc,
        (a,b) => {
          (a._1+b._1,a._2+b._2,a._3++b._3,a._4++b._4)
        }
      ).mapValues((vid,vd)=>(vd._1,vd._2,vd._3.size,vd._4.size))
    val wcc_ : Double =graphT.vertices.leftJoin(trangleInfo) {
      (vid, clu, trangleInfo ) =>
        val tInfo = trangleInfo.getOrElse(null)
        var wcc:Double = 0.0
        if (tInfo._2 != 0){
          wcc= (tInfo._1.toDouble/tInfo._2)*(tInfo._3.toDouble/(clu._2-1.0+tInfo._4))
        }
        wcc
    }.map{ case (vid,vd)=>vd}.reduce(_+_)
    wcc_ /netSize
  }
  def edgeFunc(ctx: EdgeContext[((Int,Int),Set[(Long,Int)]), Int, (Int,Int,Set[Long],Set[Long])]) {
    assert(ctx.srcAttr._2 != null)
    assert(ctx.dstAttr._2 != null)
    val srcSet=ctx.srcAttr._2
    val dstSet=ctx.dstAttr._2
    val srcCluId=ctx.srcAttr._1._1
    val dstCluId=ctx.dstAttr._1._1

    val iter = srcSet.iterator
    var counter: Int = 0
    var tInS:Int = 0
    var tInV:Int = 0
    var vxInV:Set[Long] = Set()
    var vxOutS:Set[Long]=Set()
    while (iter.hasNext) {
      val v = iter.next()
      if (v._1 != ctx.srcId && v._1 != ctx.dstId && dstSet.contains(v)) {
        if(v._2==srcCluId && v._2==dstCluId){
          tInS += 1
          tInV += 1
          vxInV = vxInV + v._1
        }else{
          tInV += 1
          vxInV = vxInV + v._1
          if(v._2!=dstCluId) {vxOutS = vxOutS + v._1}
        }
      }
    }
    ctx.sendToDst((tInS,tInV,vxInV,vxOutS))
  }




}

