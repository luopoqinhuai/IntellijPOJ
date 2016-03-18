package com.reno.recommecdation.format
/**
 * Created by reno on 2015/10/28.
 */

import com.reno.recommecdation._
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

/**
 * Created by reno on 2015/10/11.
 */

class population  (person: (Int, Array[Rating]),
                   private var ppStatistics: Map[Int, (Double, Double)],k:Int=10
                    )extends  Serializable{
  /**
   * 每一个种群有唯一的Map
   */
  val P_variation=0.3
  val parallel =80
  val logpath="hdfs://master:54321/home/spark/testlog/"
  var Is_inited=false
  var mMap: Map[Int, Double] = Map()
  val mAllItems=person._2.map { case Rating(u, i, s) => {
      mMap += (i -> s)
      i
    }
  }
  val USERID=person._1

  val D:Int=10   //recommendation number for each user

  val mRand:java.util.Random=  new java.util.Random(System.nanoTime())

  var outerPopulation=List.empty[reno_particle]

  def initialization(numbers:Int,one:RDD[Int]):RDD[reno_particle]={
    Is_inited=true
    val sc=one.sparkContext
    val broadcastmAllItems = sc.broadcast(mAllItems)
    val broadcastD = sc.broadcast(D)//broadcast variable is readonly
    val two=one.repartition(parallel).map{x=> new reno_particle(GA3.getDistinct(broadcastmAllItems.value,broadcastD.value))}
    mapFitmess(two)
  }


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def returnDist(ll:List[reno_particle]):List[reno_particle]={
    val all=ll.toArray
    var distOut=List.empty[reno_particle]
    for(i<-all){
      if(!(i in distOut.toArray)){
        distOut=i::distOut
      }
    }
    distOut
  }
  /**
   * 也可以用于更新外部种群。。。
   * @param la
   * @param lb
   * @return
   */
  def getParetofromList(la: List[reno_particle], lb: List[reno_particle]): List[reno_particle] = {
    println("reduce......")
    val la_left = la.filter(x => multiple1.IsZhiPei(x, lb))
    val lb_left = lb.filter(x => multiple1.IsZhiPei(x, la))
    returnDist(la_left ++ lb_left)//去重
  }

  /**
   * 得到非支配解
   * @param datas
   * @return
   */
  def getPareto(datas: RDD[reno_particle]): List[reno_particle] = {
    datas.map(x => List(x)).reduce(getParetofromList(_, _))  //old
    //val tmp=datas.mapPartitions{iter=>Iterator.single(iter.toList)}
     //tmp.reduce(getParetofromList(_, _))
  }
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



  /**
   * broadcast over
   * @param datas
   */
  def mapFitmess(datas: RDD[reno_particle]): RDD[reno_particle] = {
    val sc=datas.sparkContext
    val broadcast_mMap=sc.broadcast(mMap)
    val broadcast_pp=sc.broadcast(ppStatistics)
    datas.map{x=>Fitness.getFitnesses(x,broadcast_mMap.value,broadcast_pp.value)}
  }


  def oneturn(parse: RDD[reno_particle]):RDD[reno_particle]={
    val sc=parse.sparkContext
    val broadcast_P=sc.broadcast(P_variation)
    val broadcast_mAllItems=sc.broadcast(mAllItems)
    val newparse = parse.mapPartitions(iter => {
      val newchilds = GA2.select(iter)
      val newParses = newchilds.flatMap(couple => GA1.cross(couple._1, couple._2)).map{x=>GA4.variation(x,broadcast_P.value,broadcast_mAllItems.value)}
      newParses
    })
    val newparse2=mapFitmess(newparse)
    newparse2
  }




  def run(parse: RDD[reno_particle] , time:Int=10):(Int ,Array[reno_particle]) = {

    val sc=parse.sparkContext
    if(!Is_inited) require(false,"not inilizition before run")

    /**
     * 新一轮粒子的诞生
     */


    var mparse=parse
   /* val ooo1=mparse.map(x=>formatTools.checkrepeate(x.position)).reduce(_+_)
    println(s"\n\n\n chushihua repeate $ooo1 \n\n\n")*/

    for(iter<-1 to time){

      val oldparse=mparse
      val newparse=oneturn(mparse)
      newparse.persist()
      //oldparse.unpersist()
      //mparse.setName(s"itemFactors-$iter").persist()
      //选择非支配集合
      val localpareto = getPareto(newparse)                 //here reduce cause the Action
      //添加到外部种群
      outerPopulation= getParetofromList(localpareto,outerPopulation)
      //得到下一代种群
      val unionparse=(newparse union oldparse).repartition(parallel)

      mparse=unionparse.mapPartitions{
        iter=>
          val allparses=iter.toArray
          val len=allparses.length
          var outiter:Iterator[reno_particle]=Iterator.empty
          for(i<-0 to 10000/80){
            val a=mRand.nextInt(len)
            val b=mRand.nextInt(len)
            val c=mRand.nextFloat()
            val mid=0.5
            if( c<mid){
              if(allparses(a).fitness(0)>allparses(b).fitness(0)){
                outiter=outiter++Iterator(allparses(a))
              }else{
                outiter=outiter++Iterator(allparses(b))
              }
            }else{
              if(allparses(a).fitness(1)>allparses(b).fitness(1)){
                outiter=outiter++Iterator(allparses(a))
              }else{
                outiter=outiter++Iterator(allparses(b))
              }
            }
          }
          outiter
      }
    }
    val run_Out=outerPopulation.toArray
    (USERID,run_Out)
  }


}

