package com.reno.communicity.format

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
/**
 * Created by reno on 2015/11/20.
 */
class DDPSO(graph:Graph[Int,Int],vertexNum:Int)extends Serializable {

  val mRand:java.util.Random=  new java.util.Random(System.nanoTime())


  def init(one:RDD[Int],nVerticle:Int=vertexNum):RDD[cm_particle]={
    val sc=one.sparkContext
    val nVerticleBD=sc.broadcast(nVerticle)
    val particles=one.map{
      x=>
        val mRand:java.util.Random=  new java.util.Random(System.nanoTime())
        val pst=Array.fill[(Long,Int)](nVerticleBD.value){(0,mRand.nextInt(nVerticleBD.value))}
        for(i<-0 to nVerticleBD.value-1){
          val tp=pst(i)._2
          pst(i)=(i+1,tp)
        }
        val mv=Array.fill(nVerticleBD.value){mRand.nextInt(2)}
        new cm_particle(pst,mv)
    }
    val fitnessupdate=UpDateFitness(particles)
    UpdatePbestAndGbest(fitnessupdate)
  }


  def getMostFromList(datas:List[Int]):Int={
    val mmP=datas.toArray.map(x=>(x,1)).groupBy(_._1)
    val kys=mmP.keys
    var mostnum=0
    for(i<-kys){
      if(mmP(i).length>mostnum)  mostnum=mmP(i).length
    }
    var tochoose=List.empty[Int]
    for(i<-kys){
      if(mmP(i).length==mostnum)  tochoose=tochoose:+i
    }
    tochoose(mRand.nextInt(tochoose.size))
  }


  def getOneFitness(oneparticle:cm_particle,sc:SparkContext,Num:Int):cm_particle={
    val jp=sc.makeRDD(oneparticle.position)
    val tmpGraph=graph.joinVertices(jp){(vid,vdata ,U)=>U}
    val tp=tmpGraph.aggregateMessages[(Int,List[Int],Int,Int)](
      triplet => {
        if (triplet.srcAttr== triplet.dstAttr) {
          triplet.sendToDst((triplet.dstAttr,List(triplet.srcAttr),1,0))
        }else{
          triplet.sendToDst((triplet.dstAttr,List(triplet.srcAttr),0,1))
        }
      },(A,B)=>(A._1,A._2++B._2,A._3+B._3,A._4+B._4),TripletFields.All)
    val mNbest=tp.map(x=>(x._1.toInt,getMostFromList(x._2._2)))
    val pClass=tp.map(x=>(x._2._1,(x._2._3,x._2._4))).groupByKey
    val classnum=pClass.count
    val toCacufitness=pClass.map{
      case (classid,iter)=>
        val iterArr=iter.toArray
        val numVi=iterArr.length
        val (sumVi1,sumVi2)=iterArr.reduce((a,b)=>(a._1+b._1,a._2+b._2))
        (classid,sumVi1.toDouble/numVi,sumVi2.toDouble/numVi)
    }
    val Out=toCacufitness.reduce((a,b)=>(a._1,a._2+b._2,a._3+b._3))
    val NRA=(2*(Num-classnum)-Out._2)
    val RC=Out._3
    oneparticle.fitness=NRA+RC*2
    val mppp=mNbest.collect
    val nbest=Array.fill(mppp.length){-1}
    for(i<-mppp){
      nbest(i._1-1)=i._2
    }
    oneparticle.Nbest=nbest
    oneparticle
  }


  def TestgetOneFitness(position:Array[(Long,Int)],sc:SparkContext,Num:Int)={
    val jp=sc.makeRDD(position)
    val tmpGraph=graph.joinVertices(jp){(vid,vdata ,U)=>U}
    val tp=tmpGraph.aggregateMessages[(Int,List[Int],Int,Int)](
      triplet => {
        if (triplet.srcAttr== triplet.dstAttr) {
          triplet.sendToDst((triplet.dstAttr,List(triplet.srcAttr),1,0))
        }else{
          triplet.sendToDst((triplet.dstAttr,List(triplet.srcAttr),0,1))
        }
      },(A,B)=>(A._1,A._2++B._2,A._3+B._3,A._4+B._4),TripletFields.All)
    val pClass=tp.map(x=>(x._2._1,(x._2._3,x._2._4))).groupByKey
    val classnum=pClass.count
    val toCacufitness=pClass.map{
      case (classid,iter)=>
        val iterArr=iter.toArray
        val numVi=iterArr.length
        val (sumVi1,sumVi2)=iterArr.reduce((a,b)=>(a._1+b._1,a._2+b._2))
        (classid,sumVi1.toDouble/numVi,sumVi2.toDouble/numVi)
    }
    val Out=toCacufitness.reduce((a,b)=>(a._1,a._2+b._2,a._3+b._3))
    val NRA=(2*(Num-classnum)-Out._2)
    val RC=Out._3
    println("NRA "+NRA+"\tRC "+RC+"\tclassnum "+classnum)
  }


  def UpDateFitness(particles:RDD[cm_particle]):RDD[cm_particle]={
    val sc=particles.sparkContext
    val localnewparticles=particles.collect.map{
      case oneparticle=>
        val jp=sc.makeRDD(oneparticle.position)

        val tmpGraph=graph.joinVertices(jp){(vid,vdata ,U)=>U}
        //(VID,(pointClass,List(NeighbourClass),same,differ)
         val tp=tmpGraph.aggregateMessages[(Int,List[Int],Int,Int)](
            triplet => {
            if (triplet.srcAttr== triplet.dstAttr) {
              triplet.sendToDst((triplet.dstAttr,List(triplet.srcAttr),1,0))
            }else{
              triplet.sendToDst((triplet.dstAttr,List(triplet.srcAttr),0,1))
            }
         },(A,B)=>(A._1,A._2++B._2,A._3+B._3,A._4+B._4),TripletFields.All)
        val mNbest=tp.map(x=>(x._1.toInt,getMostFromList(x._2._2)))
        val pClass=tp.map(x=>(x._2._1,(x._2._3,x._2._4))).groupByKey
        val classnum=pClass.count
        val toCacufitness=pClass.map{
          case (classid,iter)=>
            val iterArr=iter.toArray
            val numVi=iterArr.length
            val (sumVi1,sumVi2)=iterArr.reduce((a,b)=>(a._1+b._1,a._2+b._2))
            (classid,sumVi1.toDouble/numVi,sumVi2.toDouble/numVi)
        }
        val Out=toCacufitness.reduce((a,b)=>(a._1,a._2+b._2,a._3+b._3))
        val NRA=(2*(vertexNum-classnum)-Out._2)
        val RC=Out._3
        oneparticle.fitness=NRA+RC*2
        val mppp=mNbest.collect
        val nbest=Array.fill(mppp.length){-1}
        for(i<-mppp){
          nbest(i._1-1)=i._2
        }
        oneparticle.Nbest=nbest
        oneparticle
    }
    particles.unpersist()
    sc.makeRDD(localnewparticles)
  }

  def getGbest(A:cm_particle,B:cm_particle):cm_particle={
    if (A.gifitness<B.gifitness) A
    else B
  }

  def UpdatePbestAndGbest(particles:RDD[cm_particle]):RDD[cm_particle]={
    val sc=particles.sparkContext
    val tmpParticles=particles.map{particle=>
    if (particle.fitness<particle.pifitness){
      particle.pifitness=particle.fitness
      particle.Pi_p=particle.position.clone()
    }
    if (particle.pifitness<particle.gifitness){
      particle.gifitness=particle.pifitness
      particle.Pg_p=particle.Pi_p.clone()
    }
    particle
    }
    val gbest=tmpParticles.reduce(getGbest(_,_))
    val gbestBD=sc.broadcast(gbest)
    tmpParticles.map { particle =>
      particle.gifitness=gbestBD.value.gifitness
      particle.Pg_p=gbestBD.value.Pi_p.clone()
      particle
    }
  }

  def XOR(A:Int,B:Int):Int={
    if (A==B) 0
    else  1
  }

  def SIG(x:Double,mmRand:java.util.Random):Boolean={
    mmRand.nextDouble()<(1/(1+math.exp(-x)))
  }

  def UpdatePandV(particles:RDD[cm_particle]):RDD[cm_particle]={
    particles.map{
      oneparticle=>
        val mRand:java.util.Random=  new java.util.Random(System.nanoTime())
        val w=mRand.nextDouble()
        val r1=1.494
        val r2=1.494
        val ll=oneparticle.speed.length
        for(i<-0 to ll-1){
          val c1=mRand.nextDouble()
          val c2=mRand.nextDouble()
          if (SIG(w*oneparticle.speed(i)+c1*r1*XOR(oneparticle.Pi_p(i)._2,oneparticle.position(i)._2)+
            +c2*r2*XOR(oneparticle.Pg_p(i)._2,oneparticle.position(i)._2),mRand)){
            oneparticle.speed(i)=1
            oneparticle.position(i)=(i+1,oneparticle.Nbest(i))
          }else{
            oneparticle.speed(i)=0
          }
        }
        oneparticle
    }
  }

   def run(particles:RDD[cm_particle],iter:Int=20):RDD[cm_particle]={
    var mParticles=UpdatePbestAndGbest(particles)
    for(i<-0 to iter){
      println("\n\n\n"+"UpdatePandV"+"\n\n\n")
      val upPV=UpdatePandV(mParticles)
      println("\n\n\n"+"UpDateFitness"+"\n\n\n")
      val fitnessupdate=UpDateFitness(upPV)
      println("\n\n\n"+"UpdatePbestAndGbest"+"\n\n\n")
      mParticles=UpdatePbestAndGbest(fitnessupdate)
    }
    mParticles
  }





}
