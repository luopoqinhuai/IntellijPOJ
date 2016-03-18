package renoformat.start

import org.apache.spark.graphx.{Graph, TripletFields}
import org.apache.spark.rdd.RDD

/**
 * Created by reno on 2016/1/15.
 */
object fitnessQ {

  def Fitness_Q(oneparticle:RDD[(Long,Int)],graph:Graph[Int,Int],MMM:Long)={
    val jp=oneparticle
    val tmpGraph=graph.joinVertices(jp){(vid,vdata ,U)=>U}

    val tp=tmpGraph.aggregateMessages[(Int,List[Int],Int,Int)](
      triplet => {
        if (triplet.srcAttr== triplet.dstAttr) {
          triplet.sendToDst((triplet.dstAttr,List(triplet.srcAttr),1,0))
        }else{
          triplet.sendToDst((triplet.dstAttr,List(triplet.srcAttr),0,1))
        }
      },(A,B)=>(A._1,A._2++B._2,A._3+B._3,A._4+B._4),TripletFields.All)
    val ch=tp.map(x=>(x._2._1,(x._2._3,x._2._4))).groupByKey()



    val deg=ch.map{case (classID,iter)=>
      val iterArr:Array[(Int,Int)]=iter.toArray
      val iterArr2=iterArr.map(x=>x._1+x._2)
      val classdeg=iterArr2.reduce{(a,b)=>a+b}
      Math.pow(classdeg,2)
    }.reduce(_+_)


    val tp2=tp.map(x=>(x._2._3))
    val ec=tp2.reduce(_+_).toDouble
    tp.unpersist(blocking = false)
    tmpGraph.unpersist(blocking = false)
    val QQQ=ec/(2*MMM)-deg/(4*MMM*MMM)
    (QQQ,ec,deg,MMM)
  }

}
