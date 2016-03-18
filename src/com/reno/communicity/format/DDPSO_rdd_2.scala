package com.reno.communicity.format

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Created by reno on 2015/11/23.
 */
class DDPSO_rdd_2 (graph:Graph[Int,Int],vertexNum:Int)extends Serializable {



  def init(sc:SparkContext,nPopulationNum:Int,nVerticle:Int=vertexNum):Array[rdd_particle2]= {
    val nVerticleBD = sc.broadcast(nVerticle)
    val mRand: java.util.Random = new java.util.Random(System.nanoTime())
    val populations: Array[rdd_particle2] = (0 to nPopulationNum - 1).toArray.map {
      x =>
        val PandV = new Array[(Long, (Int,Int))](nVerticleBD.value)
        for (i <- 0 to nVerticleBD.value - 1) {
          val tp = mRand.nextInt(nVerticleBD.value)
          val mp = mRand.nextInt(2)
          PandV(i)=(i+1,(tp,mp))
        }
        val mposition_speed = sc.makeRDD(PandV)
        val aaa = new rdd_particle2(mposition_speed)
        aaa
    }
    val fitnessupdate=populations.map(UpDateFitness(_))
/*    val fitnessupdate:Array[rdd_particle]=new Array[rdd_particle](nPopulationNum)
    for(i<-0 to nPopulationNum-1){
      fitnessupdate(i)=UpDateFitness(populations(i))
    }*/
    UpdatePbestAndGbest(fitnessupdate)
  }


  def getMostFromList(datas:List[Int]):Int={
    val mRand:java.util.Random=  new java.util.Random(System.nanoTime())
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


  def UpDateFitness(oneparticle:rdd_particle2):rdd_particle2={
    val jp=oneparticle.positionAndSpeed.map(x=>(x._1,x._2._1))
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

    val nbestold=oneparticle.Nbest
    oneparticle.Nbest=tp.map(x=>(x._1,getMostFromList(x._2._2)))
    oneparticle.Nbest.setName("Nbest").persist(StorageLevel.MEMORY_AND_DISK)
    if(nbestold!=null)  nbestold.unpersist()
      tmpGraph.unpersist()
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

        oneparticle.Fitness=NRA+RC*2
        oneparticle
  }

  def getGbest(A:rdd_particle2,B:rdd_particle2):rdd_particle2={
    if (A.gFitness<B.gFitness) A
    else B
  }

  def UpdatePbestAndGbest(particles:Array[rdd_particle2]):Array[rdd_particle2]={
    val tmpParticles=particles.map{particle=>
      if (particle.Fitness<particle.pFitness){
        val Pp_pold=particle.Pp_p
        particle.pFitness=particle.Fitness
        particle.Pp_p=particle.positionAndSpeed.map(x=>(x._1,x._2._1))
        particle.Pp_p.setName("Pp_p").persist(StorageLevel.MEMORY_AND_DISK)
        Pp_pold.unpersist()
      }
      if (particle.pFitness<particle.gFitness){
        val Pg_pold=particle.Pg_p
        particle.gFitness=particle.pFitness
        particle.Pg_p=particle.Pp_p
        particle.Pg_p.setName("Pg_p").persist(StorageLevel.MEMORY_AND_DISK)
        Pg_pold.unpersist()
      }
      particle
    }
    val gbest=tmpParticles.reduce(getGbest(_,_))
    tmpParticles.map { particle =>
      particle.gFitness=gbest.gFitness
      particle.Pg_p=gbest.Pp_p
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

  def UpdatePandV(particles:Array[rdd_particle2]):Array[rdd_particle2]={
    particles.map{
      oneparticle=>
        val joinedRDD:RDD[(Long,((((Int,Int),Int),Int),Int))]=oneparticle.positionAndSpeed.join(oneparticle.Pp_p).join(oneparticle.Pg_p).join(oneparticle.Nbest)
        val vpRDD= joinedRDD.map{
          input=>
            val mRand:java.util.Random=  new java.util.Random(System.nanoTime())
            val w=mRand.nextDouble()
            val r1=1.494
            val r2=1.494
            val c1=mRand.nextDouble()
            val c2=mRand.nextDouble()
            val tmp_position:Int=input._2._1._1._1._1
            val tmp_speed:   Int=input._2._1._1._1._2
            val tmp_pbest:   Int=input._2._1._1._2
            val tmp_gbest:   Int=input._2._1._2
            val tmp_nbest:   Int=input._2._2
            var end_speed:Int=(-1)
            var end_position:Int=(-1)
            if (SIG(w*tmp_speed+c1*r1*XOR(tmp_position,tmp_pbest)+
              +c2*r2*XOR(tmp_position,tmp_gbest),mRand)){
              end_speed=1
              end_position=tmp_nbest
            }
            else{
              end_speed=0
              end_position=tmp_position
            }
            (input._1,(end_position,end_speed))
        }
        val positionold=oneparticle.positionAndSpeed
        oneparticle.positionAndSpeed=vpRDD
        oneparticle.positionAndSpeed.setName("position").persist(StorageLevel.MEMORY_AND_DISK)
        positionold.unpersist()
        oneparticle
    }
  }
  def run(particles:Array[rdd_particle2],iter:Int=20):Array[rdd_particle2]={
    var mParticles=particles
    for(i<-0 to iter){
      println("\n\n\n"+"UpdatePandV"+"\n\n\n")
      val upPV=UpdatePandV(mParticles)
      println("\n\n\n"+"UpDateFitness"+"\n\n\n")
    /*  val fitnessupdate:Array[rdd_particle]=new Array[rdd_particle](numberOfParticles)
      for(i<-0 to numberOfParticles-1){
        fitnessupdate(i)=UpDateFitness(upPV(i))
      }*/
    val fitnessupdate=upPV.map(UpDateFitness(_))
      println("\n\n\n"+"UpdatePbestAndGbest"+"\n\n\n")
      mParticles=UpdatePbestAndGbest(fitnessupdate)
      if(iter%5==0){
      mParticles.map(x=>x.checkpoint())
      }
    }
    mParticles
  }





}

