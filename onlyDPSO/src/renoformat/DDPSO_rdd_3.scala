package renoformat

/**
 * Created by reno on 2015/12/5.
 */

import java.io.PrintWriter

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


class DDPSO_rdd_3 (graph:Graph[gen,Int],vertexNum:Int)extends Serializable {
  var populationNum=(-1)
  var lambda=0.3
  val localgraph=graph.cache()
  val localvertexNum=vertexNum
  val checkpointTimes=5
  def init(sc:SparkContext,nPopulationNum:Int,partitionNum:Int,mlambda:Double):RDD[(Long,gen)]= {
    populationNum=nPopulationNum
    lambda=mlambda
    val nVerticleBD = sc.broadcast(localvertexNum)   //节点个数
    val nPopulationNumBD = sc.broadcast(nPopulationNum)   //种群个数

    //val mRand: java.util.Random = new java.util.Random(System.nanoTime())
    val population:RDD[(Long,gen)]= sc.makeRDD(1 to localvertexNum,partitionNum).map{
      numID=>
        val mRand: java.util.Random = new java.util.Random(System.nanoTime())
        val mposition:Array[Int]=new Array[Int](nPopulationNumBD.value)
        val mspeed:Array[Int]=new Array[Int](nPopulationNumBD.value)
        val mPg_p:Array[Int]=new Array[Int](nPopulationNumBD.value)
        val mPp_p:Array[Int]=new Array[Int](nPopulationNumBD.value)
        val mNbest:Array[Int]=new Array[Int](nPopulationNumBD.value)
        val mFitness:Array[Double]=new Array[Double](nPopulationNumBD.value)
        val mpFitness:Array[Double]=new Array[Double](nPopulationNumBD.value)
        val mgFitness:Array[Double]=new Array[Double](nPopulationNumBD.value)
        for(i<-0 to nPopulationNumBD.value-1 ){
          mposition(i)=numID
          mPp_p(i)=mposition(i)
          mPp_p(i)=mPg_p(i)
          mspeed(i)=mRand.nextInt(2)
          mNbest(i)=(-1)
          mFitness(i)= Double.MaxValue
          mpFitness(i)=Double.MaxValue
          mgFitness(i)=Double.MaxValue
        }
        val mGen=new gen()
        mGen.position=mposition
        mGen.speed=mspeed
        mGen.Pg_p=mPg_p
        mGen.Pp_p=mPp_p
        mGen.Nbest=mNbest
        mGen.Fitness=mFitness
        mGen.gFitness=mgFitness
        mGen.pFitness=mpFitness
        (numID.toLong,mGen)
    }.setName("init Population").persist(StorageLevel.MEMORY_AND_DISK)
    population.count()
    val fitnessupdate=upDateFitness(population,false)
    /*    val fitnessupdate:Array[rdd_particle]=new Array[rdd_particle](nPopulationNum)
        for(i<-0 to nPopulationNum-1){
          fitnessupdate(i)=UpDateFitness(populations(i))
        }*/
    UpdatePbestAndGbest2(fitnessupdate)
  }

  def messCombine(msg1:(Array[Int],Array[List[Int]],Array[Int],Array[Int]),msg2:(Array[Int],Array[List[Int]],Array[Int],Array[Int])):(Array[Int],Array[List[Int]],Array[Int],Array[Int])={
    val len=msg1._2.length
    val newNB=new Array[List[Int]](len)
    val newSame=new Array[Int](len)
    val newDiff=new Array[Int](len)
    for(i<-0 to len-1){
      newNB(i)=msg1._2(i) ++ msg2._2(i)
      newSame(i)=msg1._3(i)+msg2._3(i)
      newDiff(i)=msg1._4(i)+msg2._4(i)
    }
    (msg1._1,newNB,newSame,newDiff)
  }

  def upDateFitness(mPoputation:RDD[(Long,gen)],isCheckpoint:Boolean):RDD[(Long,gen)]={
    require(populationNum>0)
    val sc=mPoputation.sparkContext
    val tmpGraph:Graph[gen,Int]=localgraph.joinVertices(mPoputation){(vid,vdata ,U)=>U}.cache()
    tmpGraph.vertices.count()
    tmpGraph.edges.count()
    val tp=tmpGraph.aggregateMessages[(Array[Int],Array[List[Int]],Array[Int],Array[Int])](
        triplet => {
          println(triplet.dstId+" "+triplet.srcId)
          val dstclassID=triplet.dstAttr.position.clone()
          val srcclassID=triplet.srcAttr.position.clone()
          require(dstclassID!=null&& srcclassID!=null)
          val len=dstclassID.length
          val nb:Array[List[Int]]=srcclassID.map(x=>List(x))
          val sameArr=new Array[Int](len)
          val diffArr=new Array[Int](len)
          for(i<-0 to len-1){
            if(dstclassID(i)==srcclassID(i)){
              sameArr(i)=1
            }else{
              diffArr(i)=1
            }
          }
          triplet.sendToDst((dstclassID,nb,sameArr,diffArr))
        },(A,B)=>messCombine(A,B),
        TripletFields.All)
    tp.cache()
    tp.count()
    //将一个RDD拆分成 个体数目个RDD
    val mfitness:Array[Double]=(0 to populationNum-1).map{i=>
      val pClass=tp.map(x=>(x._2._1(i),(x._2._3(i),x._2._4(i)))).groupByKey
      pClass.cache()
      pClass.count()

      val toCacufitness=pClass.map{
        case (classid,iter)=>
          val iterArr=iter.toArray
          val numVi=iterArr.length
          val (sumVi1,sumVi2)=iterArr.reduce((a,b)=>(a._1+b._1,a._2+b._2))
          (classid,sumVi1.toDouble/numVi,sumVi2.toDouble/numVi)
      }
      val Out=toCacufitness.reduce((a,b)=>(a._1,a._2+b._2,a._3+b._3))
      pClass.unpersist()
      val NRA=Out._2
      val RC=Out._3
      2*(1-lambda)*RC-NRA*2*lambda
    }.toArray
    val mfitnessBD=sc.broadcast(mfitness)

    val tmpNbest:RDD[(Long,Array[Int])]=tp.map(x=>(x._1,getMostFromArrList(x._2._2)))
    val aaa:RDD[(Long,gen)]=mPoputation.join(tmpNbest).map{
      x=>
        x._2._1.Nbest=x._2._2.clone()
        x._2._1.Fitness=mfitnessBD.value.clone()
        (x._1,x._2._1)
    }.setName("after UpdateFitness").persist(StorageLevel.MEMORY_AND_DISK)
    if(isCheckpoint) aaa.checkpoint()
    aaa.count()
    mPoputation.unpersist()
    tmpGraph.unpersistVertices()
    tmpGraph.edges.unpersist()
    tp.unpersist()
    aaa
  }

  def getBest(datas:Array[Double]):Int={
    var tmp=Double.MaxValue
    var pos=(-1)
    for(i<-0 to datas.length-1){
      if(datas(i)<tmp){
        tmp=datas(i)
        pos=i
      }
    }
    pos
  }

  def UpdatePbestAndGbest2(mPoputation:RDD[(Long,gen)])={
    val oldmPopulation=mPoputation
    val newpop=oldmPopulation.map{
      case (classID,oneGen)=>
        for(i<-0 to populationNum-1){
         if(oneGen.Fitness(i)<oneGen.pFitness(i)){
           oneGen.Pp_p(i)=oneGen.position(i)
           oneGen.pFitness(i)=oneGen.Fitness(i)
         }
          /*现在Pg_p还未统一*/
         if(oneGen.Fitness(i)<oneGen.gFitness(i)){
            oneGen.Pg_p(i)=oneGen.position(i)
            oneGen.gFitness(i)=oneGen.Fitness(i)
          }
        }
        val ps=getBest(oneGen.gFitness)
        for(i<-0 to oneGen.Pg_p.length-1 if i!=ps){
          oneGen.Pg_p(i)=oneGen.Pg_p(ps)
          oneGen.gFitness(i)=oneGen.gFitness(ps)
        }
        (classID,oneGen)
    }.setName("UpdatePbestAndGbest").persist(StorageLevel.MEMORY_AND_DISK)
    newpop.count()
    oldmPopulation.unpersist()
    newpop
  }

  def UpdatePandV2(mPoputation:RDD[(Long,gen)]):RDD[(Long,gen)]= {
    val oldone=mPoputation
    val newone=mPoputation.map {
      oneGen =>
        val mRand: java.util.Random = new java.util.Random(System.nanoTime())
        val w = mRand.nextDouble()
        val r1 = 1.494
        val r2 = 1.494
        val len = oneGen._2.position.length
        for (i <- 0 to len - 1) {
          val c1 = mRand.nextDouble()
          val c2 = mRand.nextDouble()
          val tmp_position: Int = oneGen._2.position(i)
          val tmp_speed: Int = oneGen._2.speed(i)
          val tmp_pbest: Int = oneGen._2.Pp_p(i)
          val tmp_gbest: Int = oneGen._2.Pg_p(i)
          val tmp_nbest: Int = oneGen._2.Nbest(i)
          if (SIG(w * tmp_speed + c1 * r1 * XOR(tmp_position, tmp_pbest) +
            +c2 * r2 * XOR(tmp_position, tmp_gbest), mRand)) {
            oneGen._2.speed(i) = 1
            oneGen._2.position(i) = oneGen._2.Nbest(i)
          } else {
            oneGen._2.speed(i) = 0
          }

        }
        oneGen
    }.setName("updatePandV").persist(StorageLevel.MEMORY_AND_DISK)
    newone.count()
    oldone.unpersist()
    newone
  }

  def getMostFromArrList(datas:Array[List[Int]]):Array[Int]={
    datas.map(getMostFromList(_))
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

  def XOR(A:Int,B:Int):Int={
    if (A==B) 0
    else  1
  }

  def SIG(x:Double,mmRand:java.util.Random):Boolean={
    mmRand.nextDouble()<(1/(1+math.exp(-x)))
  }

  def run(particles:RDD[(Long,gen)],iter:Int=20):RDD[(Long,gen)]={
    var mParticles=particles
    var onetrunTime=0.0
    val writep=new PrintWriter("/home/spark/reno/iter_checkpoint_2.txt")
    var docheckpoint=0
    var ischeckpoint=false;
    for(i<-0 to iter){
      val startTime=System.currentTimeMillis()
      val upPV=UpdatePandV2(mParticles)
      if(docheckpoint>=checkpointTimes){
        ischeckpoint=true
        docheckpoint=0
      }

      val startTime_fitness=System.currentTimeMillis()
      val fitnessupdate=upDateFitness(upPV,ischeckpoint)
      ischeckpoint=false
      val fitnesstime=System.currentTimeMillis()-startTime_fitness
      mParticles=UpdatePbestAndGbest2(fitnessupdate)
      onetrunTime=System.currentTimeMillis()-startTime

      writep.write(s"iter num $i takes "+onetrunTime+s" ms    end   fitness takse   $fitnesstime ms\n")
      docheckpoint+=1

    }
    writep.close()
    mParticles
  }





}



