package com.reno.test.format

/**
 * Created by reno on 2015/10/11.
 */
class pase_Particle(var poisition: Array[Int])  {
  var ALL_Fitness: Array[Float] = null

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
  def cross(other: pase_Particle): (pase_Particle,pase_Particle) = {
    val mMax = poisition.length
    val seed = System.nanoTime();
    val mRandom: java.util.Random = new java.util.Random(seed)
    val one = 4//mRandom.nextInt(mMax)
    val two = 5//mRandom.nextInt(mMax)
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
    val child_1=this.poisition.clone()
    val child_2=other.poisition.clone()
    /**
     * start < end
     */
    if (start < end) {
      println(s"start  $start   end  $end")
      /**
       * change from start to end
       */
      for (i <- 0 to end - start ) {
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
    (new pase_Particle(child_1),new pase_Particle(child_2))
  }

  override def toString :String={
    var str=""
    for(i<-this.poisition){
      str+=i+" "
    }
    str
  }


}
