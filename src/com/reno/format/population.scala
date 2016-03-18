package com.reno.format


import org.apache.spark.SparkContext
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
  val P_variation=0.2
  val parallel =20
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





  def getDistinct(datas:Array[Int],len:Int):Array[Int]={
    var  flag:Boolean=true
    val out=Array.fill[Int](len){(-1)}
    for(i<-0 to len-1){
          while(flag){
            val enum=mRand.nextInt(datas.length)
            if(!out.contains(datas(enum))){
              out(i)=datas(enum)
              flag=false
            }
      }
      flag=true
    }
    out
  }

  def initialization(numbers:Int,one:RDD[Int]):RDD[reno_particle]={
    Is_inited=true
   // val one=sparkcontext.parallelize(1 to numbers,10)

    val two=one.repartition(parallel).map{x=> new reno_particle(getDistinct(mAllItems,D))}
      two.map(getFitnesses(_))
    //并行度20

  }



  /**
   * check if there are any repeate enum
   * @param arr
   * @param start
   * @param end
   * @return
   */
  def check(arr: Array[Int], start: Int, end: Int): (Int, Int) = {
    require(start < end, s"need start less than end but start=$start ,end=$end")
    require(arr.length >= end, s"end is over Array :end=$end")
    var is_repeate = false
    var Outindex: Int = (-1)
    var Inindex: Int = (-1)
    for (i <- start to end if !is_repeate) {
      for (j <- 0 to arr.length - 1 if !is_repeate && j < start || j > end) {
        if (arr(i) == arr(j)) {
          Outindex = j
          Inindex = i
          is_repeate = true
        }
      }
    }
    (Inindex, Outindex)
  }

  def cross(One: reno_particle, other: reno_particle): Array[reno_particle] = {
    val mMax = other.position.length
    val seed = System.nanoTime();
    val mRandom: java.util.Random = new java.util.Random(seed)
    val one = mRandom.nextInt(mMax)
    val two = mRandom.nextInt(mMax)
    var start = 0
    var end = 0
    if (one > two) {
      start = two
      end = one
    }
    else if (one < two) {
      start = one
      end = two
    }
    val child_1 = One.position.clone()
    val child_2 = other.position.clone()

    /**
     * start < end
     */
    if (start < end) {
      println(s"start  $start   end  $end")

      /**
       * change from start to end
       */
      for (i <- 0 to end - start) {
        val tmp = child_1(start + i)
        child_1(start + i) = child_2(start + i)
        child_2(start + i) = tmp
      }
      var notJump = true
      while (notJump) {
        val rep = check(child_1, start, end)
        if (rep._1 != (-1)) {
          println(s"find repeate place $rep ,  $rep")
          child_1(rep._2) = child_2(rep._1)
        } else {
          notJump = false
        }
      }
      notJump = true
      while (notJump) {
        val rep = check(child_2, start, end)
        if (rep._1 != (-1)) {
          println(s"find repeate place $rep ")
          child_2(rep._2) = child_1(rep._1)
        } else {
          notJump = false
        }
      }
    }

    /**
     * start ==end
     */
    else {
      val tmp = child_1(start)
      child_1(start) = child_2(start)
      child_2(start) = tmp
    }
    Array(new reno_particle(child_1), new reno_particle(child_2))
  }


  def variation(abc:reno_particle): reno_particle ={
    if(mRand.nextFloat()<P_variation) {
      var flag = true
      val len = mAllItems.length
      val mlen = abc.position.length
      while (flag) {
        val one = mAllItems(mRand.nextInt(len))
        if (!abc.position.contains(one)) {
          abc.position(mRand.nextInt(mlen)) = one
          flag = false
        }
      }
    }
    abc
  }

  /**
   * 从sub_population中选择 N 对   2*N为sub_population的规模
   * @param sub_population
   * @return
   */
  def select(sub_population: Iterator[reno_particle]): Iterator[(reno_particle, reno_particle)] = {
    val _sub_population=sub_population.toArray
    val choice=_sub_population.length
    (1 to  choice/2).map{x=>Iterator((_sub_population(mRand.nextInt(choice)),_sub_population(mRand.nextInt(choice))))}.reduce(_++_)
  }


  /**
   * 被支配 返回false
   * @param a
   * @param lb
   * @return
   */
  def IsZhiPei(a: reno_particle, lb: List[reno_particle]): Boolean = {
    var out = true
    for (b <- lb if out) {
      if (b.fitness(0) > a.fitness(0) && b.fitness(1) > a.fitness(1)) {
        out = false
      }
    }
    out
  }



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
    val la_left = la.filter(x => IsZhiPei(x, lb))
    val lb_left = lb.filter(x => IsZhiPei(x, la))
    returnDist(la_left ++ lb_left)//去重
  }

  /**
   * 得到非支配解
   * @param datas
   * @return
   */
  def getPareto(datas: RDD[reno_particle]): List[reno_particle] = {
    datas.map(x => List(x)).reduce(getParetofromList(_, _))
  }

  /**
   * get fitness
   */
  def getFitnesses(particle: reno_particle): reno_particle = {
    val f1: Double = particle.position.map { x: Int => mMap.getOrElse(x, (-1.0)) }.sum
    //val Rating = RatingStatistics.meanAndVariance(parti)
    val f2: Double = f2func(particle.position)
    particle.fitness(0) = f1
    particle.fitness(1) = f2
    particle
  }
  private def f2func(particle: Array[Int]): Double = {
    var f2:Double = 0
    var mv:(Double,Double) = (0,0)
    for (ir <- particle){
      mv = ppStatistics(ir)
      f2 = f2 + mv._1*math.pow((mv._2+1),2)
    }
    1.0/f2
  }




  def mapFitmess(datas: RDD[reno_particle]): Unit = {
    datas.map(getFitnesses(_))
  }

  /**
   *
   * @param sc
   */
  def test(sc: SparkContext) :Unit= {
    val TestData = Array(
      (1, 56), (3, 23), (7, 2), (76, 1), (1, 10), (4, 3), (3, 2), (5, 1), (12, 6)
    )
    val data = sc.parallelize(TestData).map {
      x => {
        val pp = new reno_particle(Array(1, 2, 3))
        pp.fitness(0) = x._1
        pp.fitness(1) = x._2
        pp
      }
      //getPareto(data)
    }
  }

  def run(parse: RDD[reno_particle] , time:Int=10):(Int ,Array[reno_particle]) = {

    if(!Is_inited) require(false,"not inilizition before run")
    /**
     * 新一轮粒子的诞生
     */
    var iterate=time

    while (iterate > 0) {
      iterate-=1
      val newparse = parse.mapPartitions(iter => {
        val newchilds = select(iter)
        val newParses = newchilds.flatMap(couple => cross(couple._1, couple._2)).map(variation(_)) //这里加上变异
        newParses
        //Iterator.single(newParses)
      }).map(getFitnesses(_))
      //选择非支配集合
      val localpareto = getPareto(newparse)
      //添加到外部种群
      outerPopulation= getParetofromList(localpareto,outerPopulation)
    }

    val run_Out=outerPopulation.toArray
    println(run_Out)

    (USERID,run_Out)


    //RDD[reno_particle]
    //sc.parallelize(outerPopulation).saveAsObjectFile(logpath+System.nanoTime())
    //逆运算sc.objectFile[reno_particle](path)


  }


}
