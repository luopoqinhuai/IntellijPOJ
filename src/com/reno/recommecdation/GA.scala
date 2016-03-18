package  com.reno.recommecdation
import com.reno.recommecdation.format.reno_particle

/**
 * Created by reno on 2015/11/2.
 */

object GA1 extends Serializable{

  def check(arr: Array[Int], start: Int, end: Int): (Int, Int) = {
    require(start <= end, s"need start less than end but start=$start ,end=$end")
    require(arr.length >= end, s"end is over Array :end=$end")

    //println("check "+formatTools.ArrToStr(arr))
    var is_repeate = false
    var Outindex: Int = (-1)
    var Inindex: Int = (-1)
    for (i <- start to end if !is_repeate) {
      for (j <- 0 to arr.length - 1 if !is_repeate && j<start && j>end) {
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
    val child_1_old=child_1.clone()
    val child_2_old=child_2.clone()

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
          var s1=""
          var s2=""
          for(i<-0 to child_1.length-1 ){
            s1+=child_1(i)+" "
            s2+=child_2(i)+" "
          }
          child_1(rep._2)=child_1_old(rep._1)
        } else {
          notJump = false
        }
      }
      notJump = true
      while (notJump) {
        val rep = check(child_2, start, end)
        if (rep._1 != (-1)) {
          child_2(rep._2)=child_2_old(rep._1)
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
      var notJump = true
      while (notJump) {
        val rep = check(child_1, start, end)
        if (rep._1 != (-1)) {
          child_1(rep._2) = child_2(rep._1)
        } else {
          notJump = false
        }
      }
      notJump = true
      while (notJump) {
        val rep = check(child_2, start, end)
        if (rep._1 != (-1)) {
          child_2(rep._2) = child_1(rep._1)
        } else {
          notJump = false
        }
      }
    }
    Array(new reno_particle(child_1), new reno_particle(child_2))
  }
}

object GA2 extends  Serializable{
  val mRand:java.util.Random=  new java.util.Random(System.nanoTime())
  def select(sub_population: Iterator[reno_particle]): Iterator[(reno_particle, reno_particle)] = {
    val _sub_population=sub_population.toArray
    val choice=_sub_population.length
    (1 to  choice/2).map{x=>Iterator((_sub_population(mRand.nextInt(choice)),_sub_population(mRand.nextInt(choice))))}.reduce(_++_)
  }
}




object GA3 extends Serializable{

  def getDistinct(datas:Array[Int],len:Int):Array[Int]={
    val mRand:java.util.Random=  new java.util.Random(System.nanoTime())
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
}



object Fitness extends Serializable{
  def getFitnesses(particle: reno_particle,mmMap: Map[Int, Double],pppStatistics: Map[Int, (Double, Double)]): reno_particle = {
    val f1: Double = particle.position.map { x: Int => mmMap.getOrElse(x, (-1.0)) }.sum
    //val Rating = RatingStatistics.meanAndVariance(parti)
    val f2: Double = f2func(particle.position,pppStatistics)
    particle.fitness(0) = f1
    particle.fitness(1) = f2
    particle
  }
  private def f2func(particle: Array[Int],pppStatistics: Map[Int, (Double, Double)]): Double = {
    var f2:Double = 0
    var mv:(Double,Double) = (0,0)
    for (ir <- particle){
      mv = pppStatistics(ir)
      f2 = f2 + 1.0/(mv._1*math.pow((mv._2+1),2))
    }
    f2
  }

}


object GA4 extends Serializable{
  def variation(abc:reno_particle,P_variation:Double,mAllItems:Array[Int]): reno_particle ={
    val mRand:java.util.Random=  new java.util.Random(System.nanoTime())
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
}


object multiple1 extends  Serializable{
  def IsZhiPei(a: reno_particle, lb: List[reno_particle]): Boolean = {
    var out = true
    for (b <- lb if out) {
      if (b.fitness(0) > a.fitness(0) && b.fitness(1) > a.fitness(1)) {
        out = false
      }
    }
    out
  }
}

/*object multiple2 extends  Serializable{
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

}*/






