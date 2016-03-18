package others

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{GraphLoader, Graph}

/**
 * Created by reno on 2016/1/11.
 */
class testtttt {

  val sc = new SparkContext()

  //val fpath="/home/spark/community_detection/EdgeEmail-Enron.txt"
  val fpath="/home/spark/community_detection/Edgefootball.txt"
  val mgraph=GraphLoader.edgeListFile(sc, fpath)

  renoK_core(sc,mgraph,10)

  def renoK_core(sc:SparkContext,network:Graph[Int,Int],k:Int)= {
    var flag=true
    var badflag=false
    var thisNet=network
    var oldNet:Graph[Int,Int]=null

    while(flag){
      val deg=thisNet.degrees

      val len=deg.count()
      val summ=deg.map(x=>if(x._2>=k*2) 1 else 0).reduce(_+_)
      println("\n\n\n\n\n"+len+"\n\n\n\n\n")

      if(summ==len||len<k){
        flag=false
        if(len<k) badflag=true
      }
      else{

        val joinDegrees=thisNet.joinVertices(deg){(vid,vdata ,U)=>U}.cache()
        oldNet=thisNet
        thisNet=joinDegrees.subgraph(e=>true,(vid,degs)=>degs>=k*2)
        thisNet.cache()
        joinDegrees.edges.count()
        joinDegrees.vertices.count()
        thisNet.edges.count()
        thisNet.vertices.count()
        joinDegrees.vertices.unpersist(blocking = false)
        oldNet.unpersistVertices(blocking = false)
        oldNet.unpersist(blocking = false)
      }
    }
    if(badflag){
      println(s"can not find such graph k=$k is too large!!!")
      null
    }else   thisNet
  }
}
