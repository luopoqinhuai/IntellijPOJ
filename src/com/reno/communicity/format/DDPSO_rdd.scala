package com.reno.communicity.format

import java.io.{PrintWriter, File}


import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.io.Source

/**
 * Created by reno on 2015/11/23.
 */
class DDPSO_rdd (graph:Graph[Int,Int],vertexNum:Int)extends Serializable {
/*
  def init_old(sc:SparkContext,nPopulationNum:Int,nVerticle:Int=vertexNum):Array[rdd_particle]= {
    val nVerticleBD = sc.broadcast(nVerticle)
    val mRand: java.util.Random = new java.util.Random(System.nanoTime())
    val populations: Array[rdd_particle] = (0 to nPopulationNum - 1).toArray.map {
      x =>
        val pst = new Array[(Long, Int)](nVerticleBD.value)
        val mv = new Array[(Long, Int)](nVerticleBD.value)
        for (i <- 0 to nVerticleBD.value - 1) {
          val tp = mRand.nextInt(nVerticleBD.value)
          val mp = mRand.nextInt(2)
          pst(i) = (i + 1, tp)
          mv(i) = (i + 1, mp)
        }
        val mposition = sc.makeRDD(pst).setName("position0").persist(StorageLevel.MEMORY_AND_DISK)
        mposition.foreach(x => {})
        val mspeed = sc.makeRDD(mv).setName("speed0").persist(StorageLevel.MEMORY_AND_DISK)
        mspeed.foreach(x => {})
        val aaa = new rdd_particle(mposition, mspeed)
        aaa
    }
    val fitnessupdate=populations.map(x=>UpDateFitness_D(x,lambda = 0.3,iter=0))
/*    val fitnessupdate:Array[rdd_particle]=new Array[rdd_particle](nPopulationNum)
    for(i<-0 to nPopulationNum-1){
      fitnessupdate(i)=UpDateFitness(populations(i))
    }*/
    UpdatePbestAndGbest(fitnessupdate)
  }
*/
/*
  def testFitness(sc:SparkContext,particle:Array[Int],mgraph:Graph[Int,Int],MMM:Int):Double={
    val pl=particle.length
    val class_partiucle=new Array[(Long,Int)](pl)
    for(i<-1 to pl){
      class_partiucle(i-1)=(i,particle(i-1))
    }
    val jp=sc.makeRDD(class_partiucle)
    val tmpGraph=graph.joinVertices(jp){(vid,vdata ,U)=>U}

    val tp=tmpGraph.aggregateMessages[(Int,(Int,Int))](
      triplet => {
        if (triplet.srcAttr== triplet.dstAttr) {
          triplet.sendToDst((triplet.dstAttr,(1,0)))
        }else{
          triplet.sendToDst((triplet.dstAttr,(0,1)))
        }
      },(A,B)=>(A._1,(A._2._1+B._2._1,A._2._2+B._2._2)),TripletFields.All)
    val ch=tp.map(x=>x._2).groupByKey()

    val deg=ch.map{case (classID,iter)=>
      val iterArr:Array[(Int,Int)]=iter.toArray
      val iterArr2=iterArr.map(x=>x._1+x._2)
      val classdeg=iterArr2.reduce{(a,b)=>a+b}
      Math.pow(classdeg,2)
    }.reduce(_+_)

    val tp2=tp.map(x=>(x._2._2._1))
    val ec=tp2.reduce(_+_).toDouble
    println(ec+" "+deg)
    println("q1 "+(1-ec/MMM)+"   q2 "+deg/(4*MMM*MMM))
    1-ec/MMM+deg/(4*MMM*MMM)
  }
  def testFitness2(sc:SparkContext,particle:Array[Int],mgraph:Graph[Int,Int],MMM:Int):Double={
    val pl=particle.length
    val class_partiucle=new Array[(Long,Int)](pl)
    for(i<-1 to pl){
      class_partiucle(i-1)=(i,particle(i-1))
    }
    val jp=sc.makeRDD(class_partiucle)
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
    tmpGraph.unpersistVertices(blocking = false)
    println("q1 "+(1-ec/(2*MMM))+"   q2 "+deg/(4*MMM*MMM))
    1-ec/(2*MMM)+deg/(4*MMM*MMM)
  }

  def testFitness_D(graph:Graph[Int,Int],sc:SparkContext,position:Array[(Long,Int)],lambda:Double=0.5)={
    val jp=sc.makeRDD(position)
    val tmpGraph:Graph[Int,Int]=graph.joinVertices(jp){(vid,vdata ,U)=>U}
    //(VID,(pointClass,List(NeighbourClass),same,differ)
    val tp=tmpGraph.aggregateMessages[(Int,List[Int],Int,Int)](
      triplet => {
        if (triplet.srcAttr== triplet.dstAttr) {
          triplet.sendToDst((triplet.dstAttr,List(triplet.srcAttr),1,0))
        }else{
          triplet.sendToDst((triplet.dstAttr,List(triplet.srcAttr),0,1))
        }
      },(A,B)=>(A._1,A._2++B._2,A._3+B._3,A._4+B._4),TripletFields.All)

    val pClass=tp.map(x=>(x._2._1,(x._2._3,x._2._4))).groupByKey

    val toCacufitness=pClass.map{
      case (classid,iter)=>
        val iterArr=iter.toArray
        val numVi=iterArr.length
        val (sumVi1,sumVi2)=iterArr.reduce((a,b)=>(a._1+b._1,a._2+b._2))
        (classid,sumVi1.toDouble/numVi,sumVi2.toDouble/numVi)
    }
    val Out=toCacufitness.reduce((a,b)=>(a._1,a._2+b._2,a._3+b._3))
    val NRA=Out._2
    val RC=Out._3
    tp.unpersist(blocking = false)
   2*(1-lambda)*RC-NRA*2*lambda

  }

  def UpDateFitness_Q(oneparticle:rdd_particle,MMM:Int,iter:Int=0):rdd_particle={
    val jp=oneparticle.position
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
    oneparticle.Nbest=tp.map(x=>(x._1,getMostFromList(x._2._2))).setName("Nbest"+iter).persist(StorageLevel.MEMORY_AND_DISK)
    if(nbestold!=null)  nbestold.unpersist(blocking = false)
    oneparticle.Nbest.foreach(x => {})
    oneparticle.Nbest.checkpoint()
    oneparticle.Nbest.foreach(x => {})
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
    oneparticle.Fitness=1-ec/(2*MMM)+deg/(4*MMM*MMM)
    oneparticle
  }
*/


  def init_tmp_olp(sc:SparkContext,nPopulationNum:Int,nVerticle:Int=vertexNum):Array[rdd_particle]= {

    val populations: Array[rdd_particle] = (0 to nPopulationNum - 1).toArray.map {
      x =>
        val mposition = sc.makeRDD((1 to nVerticle).map(x=>(x.toLong,x)).toArray).repartition(200).setName("position0").persist(StorageLevel.MEMORY_AND_DISK)
        mposition.foreach(x => {})
        val mspeed = sc.makeRDD((1 to nVerticle).map(x=>(x.toLong,1)).toArray).repartition(200).setName("speed0").persist(StorageLevel.MEMORY_AND_DISK)
        mspeed.foreach(x => {})
        val aaa = new rdd_particle(mposition, mspeed)
        aaa
    }
    val fitnessupdate=populations.map(x=>UpDateFitness_D(x,lambda = 0.3,iter=0))
    UpdatePbestAndGbest(fitnessupdate)
  }

  def init(sc:SparkContext,nPopulationNum:Int,nVerticle:Int=vertexNum):Array[rdd_particle]= {
    val nVerticleBD = sc.broadcast(nVerticle)
    val mRand: java.util.Random = new java.util.Random(System.nanoTime())
    val populations: Array[rdd_particle] = (0 to nPopulationNum - 1).toArray.map {
      x =>
        val pst = new Array[(Long, Int)](nVerticleBD.value)
        val mv = new Array[(Long, Int)](nVerticleBD.value)
        for (i <- 0 to nVerticleBD.value - 1) {
          val tp = mRand.nextInt(nVerticleBD.value)
          val mp = mRand.nextInt(2)
          pst(i) = (i + 1, tp)
          mv(i) = (i + 1, mp)
        }
        val mposition = sc.makeRDD(pst).setName("position0").persist(StorageLevel.MEMORY_AND_DISK)
        mposition.foreach(x => {})
        val mspeed = sc.makeRDD(mv).setName("speed0").persist(StorageLevel.MEMORY_AND_DISK)
        mspeed.foreach(x => {})
        val aaa = new rdd_particle(mposition, mspeed)
        aaa
    }
    val fitnessupdate=populations.map(x=>UpDateFitness_D(x,lambda = 0.3))
    /*    val fitnessupdate:Array[rdd_particle]=new Array[rdd_particle](nPopulationNum)
        for(i<-0 to nPopulationNum-1){
          fitnessupdate(i)=UpDateFitness(populations(i))
        }*/
    UpdatePbestAndGbest(fitnessupdate)
  }


  /**
   * 存档前记得清楚数据
   * @param particles
   */
  def storeALLparticles(particles:Array[rdd_particle])={
    val sc=particles(0).Pg_p.sparkContext
    val allnum=particles.length
    for(i<-0 to allnum-1){
      particles(i).position.saveAsObjectFile("/home/spark/renostore/position_"+i+"_tmp.rdd")
      particles(i).Pg_p.saveAsObjectFile("/home/spark/renostore/Pg_p_"+i+"_tmp.rdd")
      particles(i).Pp_p.saveAsObjectFile("/home/spark/renostore/Pp_p_"+i+"_tmp.rdd")
      particles(i).Nbest.saveAsObjectFile("/home/spark/renostore/Nbest"+i+"_tmp.rdd")
      particles(i).speed.saveAsObjectFile("/home/spark/renostore/speed_"+i+"_tmp.rdd")
    }
    val writer = new PrintWriter(new File("/home/spark/reno/BIG_out.fitness" ))
    for(i<-0 to allnum-1){
      val ottt=particles(i).Fitness+"\t"+particles(i).pFitness+"\t"+particles(i).gFitness+"\n"
      writer.write(ottt)
    }
    writer.close()
  }


  def reBuildparticles(sc:SparkContext,allnum:Int):Array[rdd_particle]={
    val outParticles=new Array[rdd_particle](allnum)
    val arr=Source.fromFile("/home/spark/reno/BIG_out.fitness").getLines.toArray
    val threevalues=arr.map{x=> val sp=x.split("\t"); (sp(0).toDouble,sp(1).toDouble,sp(2).toDouble)}
    for(i<-0 to allnum-1){
      val tmpP=sc.objectFile[(Long,Int)]("/home/spark/renostore/position_"+i+"_tmp.rdd").cache()
      val tmpS=sc.objectFile[(Long,Int)]("/home/spark/renostore/speed_"+i+"_tmp.rdd").cache()
      val tmpPg=sc.objectFile[(Long,Int)]("/home/spark/renostore/Pg_p_"+i+"_tmp.rdd").cache()
      val tmpPp_p=sc.objectFile[(Long,Int)]("/home/spark/renostore/Pp_p_"+i+"_tmp.rdd").cache()
      val tmpNbest=sc.objectFile[(Long,Int)]("/home/spark/renostore/Nbest"+i+"_tmp.rdd").cache()
      val oneparticle = new rdd_particle(tmpP,tmpS)
      oneparticle.Pg_p=tmpPg
      oneparticle.Pp_p=tmpPp_p
      oneparticle.Nbest=tmpNbest
      oneparticle.Fitness=threevalues(i)._1
      oneparticle.pFitness=threevalues(i)._2
      oneparticle.gFitness=threevalues(i)._3
      outParticles(i)=oneparticle
    }
    outParticles
  }




  def cacuNMI(CA:RDD[(Long,Int)],CB:RDD[(Long,Int)]):Double={
    val NNN1=CA.count()
    val NNN2=CB.count()
    require(NNN1==NNN2)
    val CA_1:RDD[(Int,Double)]=CA.map(x=>(x._2,x._1)).groupByKey().map(x=>(x._1,x._2.size.toDouble))
    val CB_1:RDD[(Int,Double)]=CB.map(x=>(x._2,x._1)).groupByKey().map(x=>(x._1,x._2.size.toDouble))
    val down1=CA_1.map(x=>x._2).map(x=>x*math.log10(x/NNN1)).reduce(_+_)
    val down2=CB_1.map(x=>x._2).map(x=>x*math.log10(x/NNN1)).reduce(_+_)
                  //classA,classB,cab
    val CAB_1:RDD[((Int,Int),Double)]=CA.join(CB).map(x=>(x._2,x._1)).groupByKey().map(x=>(x._1,x._2.size.toDouble))
            //classA classB cab      ca
    val jn:RDD[(Int,((Int,Double),Double))]=CAB_1.map(x=>(x._1._1,(x._1._2,x._2))).join(CA_1)
               //classB  cab   ca
    val jn2:RDD[(Int,(Double,Double))]=jn.map(x=>(x._2._1._1,(x._2._1._2,x._2._2)))
              //classB    cab   ca     cb
    val jn3:RDD[(Int,((Double,Double),Double))]=jn2.join(CB_1)
    val up=jn3.map(x=>(x._2._1._1,x._2._1._2,x._2._2)).map(x=>x._1*math.log10(x._1*NNN1/(x._2*x._3))).reduce(_+_)
    /*val up=CA.join(CB).map(x=>(x._2,x._1)).groupByKey().map(x=>x._2.size.toDouble).map(x=>x*math.log10(x/NNN1)).reduce(_+_)*/
    -2*up/(down1+down2)
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

  def UpDateFitness_D(oneparticle:rdd_particle,lambda:Double=0.5,iter:Int=(-1)):rdd_particle={
    val jp=oneparticle.position
    val tmpGraph:Graph[Int,Int]=graph.joinVertices(jp){(vid,vdata ,U)=>U}.cache()
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
    oneparticle.Nbest=tp.map(x=>(x._1,getMostFromList(x._2._2))).setName("Nbest"+iter).persist(StorageLevel.MEMORY_AND_DISK)
    if(iter%3==0) oneparticle.Nbest.checkpoint()
    oneparticle.Nbest.foreach(x => {})

    val pClass=tp.map(x=>(x._2._1,(x._2._3,x._2._4))).groupByKey
    val toCacufitness=pClass.map{
          case (classid,iter)=>
            val iterArr=iter.toArray
            val numVi=iterArr.length
            val (sumVi1,sumVi2)=iterArr.reduce((a,b)=>(a._1+b._1,a._2+b._2))
            (classid,sumVi1.toDouble/numVi,sumVi2.toDouble/numVi)
    }
    val Out=toCacufitness.reduce((a,b)=>(a._1,a._2+b._2,a._3+b._3))
    val NRA=Out._2
    val RC=Out._3

    if(nbestold!=null)  nbestold.unpersist(blocking = false)
    tp.unpersist(blocking = false)
    tmpGraph.unpersistVertices(blocking = false)
    tmpGraph.edges.unpersist(blocking = false)
    println("  iter is"+iter+"   fitness is "+(2*(1-lambda)*RC-NRA*2*lambda))
    oneparticle.Fitness=2*(1-lambda)*RC-NRA*2*lambda
    oneparticle
  }

  def getGbest(A:rdd_particle,B:rdd_particle):rdd_particle={
    if (A.gFitness<B.gFitness) A
    else B
  }

  def UpdatePbestAndGbest(particles:Array[rdd_particle],iter:Int=(-1)):Array[rdd_particle]={
    val tmpParticles=particles.map{particle=>
      if (particle.Fitness<particle.pFitness){
        val Pp_pold=particle.Pp_p
        particle.pFitness=particle.Fitness
        particle.Pp_p=particle.position.map(x=>x).setName("Pp_p"+iter).persist(StorageLevel.MEMORY_AND_DISK)
        if(iter%5==0) particle.Pp_p.checkpoint()
        particle.Pp_p.foreach(x => {})
        Pp_pold.unpersist(blocking = false)
      }

      if (particle.pFitness<particle.gFitness){
        val Pg_pold=particle.Pg_p
        particle.gFitness=particle.pFitness
        particle.Pg_p=particle.Pp_p.map(x=>x).setName("Pg_p"+iter).persist(StorageLevel.MEMORY_AND_DISK)
        particle.Pg_p.foreach(x => {})
        Pg_pold.unpersist(blocking = false)
      }
      particle
    }
    val gbest:rdd_particle=tmpParticles.reduce(getGbest(_,_))

    val outParticles:Array[rdd_particle]=tmpParticles.map { particle =>
      particle.gFitness=gbest.gFitness
      val Pg_pold:RDD[(Long,Int)]=particle.Pg_p
      particle.Pg_p=gbest.Pg_p.map(x=>x).setName("Pg_p"+iter).persist(StorageLevel.MEMORY_AND_DISK)
      if(iter%5==0) particle.Pg_p.checkpoint()
      particle.Pg_p.foreach(x => {})
      Pg_pold.unpersist(blocking = false)
      particle
    }

    outParticles
  }

  def XOR(A:Int,B:Int):Int={
    if (A==B) 0
    else  1
  }

  def SIG(x:Double,mmRand:java.util.Random):Boolean={
    mmRand.nextDouble()<(1/(1+math.exp(-x)))
  }

  def UpdatePandV(particles:Array[rdd_particle],iter:Int=(-1)):Array[rdd_particle]={
    particles.map{
      oneparticle=>
        val joinedRDD:RDD[(Long,((((Int,Int),Int),Int),Int))]=oneparticle.position.join(oneparticle.speed).join(oneparticle.Pp_p).join(oneparticle.Pg_p).join(oneparticle.Nbest)
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
            (input._1,end_position,end_speed)
        }
        val positionold=oneparticle.position
        val speedold=oneparticle.speed
        oneparticle.position=vpRDD.map(x=>(x._1,x._2)).setName("position"+iter).persist(StorageLevel.MEMORY_AND_DISK)
        if(iter%5==0) oneparticle.position.checkpoint()
        oneparticle.position.foreach(x => {})

        oneparticle.speed=vpRDD.map(x=>(x._1,x._3)).setName("speed"+iter).persist(StorageLevel.MEMORY_AND_DISK)
        if(iter%5==0) oneparticle.speed.checkpoint()
        oneparticle.speed.foreach(x => {})

        positionold.unpersist(blocking = false)
        speedold.unpersist(blocking = false)
        oneparticle
    }
  }


  def run(particles:Array[rdd_particle],startiter:Int,enditer:Int):Array[rdd_particle]={
    var mParticles=particles
    for(i<-startiter to enditer){
      val pB=UpdatePandV(mParticles,i)
      val pA=pB.map(x=>UpDateFitness_D(x,lambda = 0.3,iter=i))
      mParticles=UpdatePbestAndGbest(pA,i)
    }
    mParticles
  }





}

