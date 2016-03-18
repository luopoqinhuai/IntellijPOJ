package com.reno.recommecdation.judge

import com.reno.recommecdation.format.reno_particle
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by reno on 2015/11/12.
 */
object Testjudge {


  def getSamenum(REC:Array[Int],Base:Array[Int]):Int={
    var num=0
    for(i<-REC){
      if(Base.contains(i)){
        num+=1
      }
    }
    num
  }

  def findBast(D:(Int,((Array[(Int,Double)]),Array[reno_particle]))):(Array[Int],Int)={
    val realarr=D._2._1.map{case (id,score)=>id}
    val recarr=D._2._2.map(x=>x.position)
    var Bestnum=0
    var Bestpoisition=recarr(0)
    for(i<-recarr){
      val tmp = getSamenum(i,realarr)
      if (tmp>Bestnum){
        Bestpoisition=i
        Bestnum=tmp
      }
    }
    (Bestpoisition,Bestnum)
  }

  def main(args: Array[String]) {
    val pathtest = "hdfs://master:54321/home/spark/recommend_system/ml-100k/u1.test"
    val splitCode = "\t";
    val conf = new SparkConf().setAppName("renoALS_Judge_100k")
    val sc = new SparkContext(conf)
    //(userid,(itemid,socres))
    val testdatas = sc.textFile(pathtest).map {
    x => {
      val arr = x.split(splitCode);
      Array(arr(0), arr(1), arr(2))
    }
  }.map{
      case Array(user,item,score)=>
        (user.toInt,(item.toInt,score.toDouble)
          )
    }.groupByKey.map{
      x=>{
        (x._1,x._2.toArray)
      }
    }
    testdatas.cache()

    val recommendDatas=sc.objectFile[(Int, Array[reno_particle])]("hdfs://master:54321/home/spark/testlog/RECOMMENDATION285254117912014")

    val judgeDatas=testdatas.join(recommendDatas)
    //out  做好的选解得到的结果
    val out=judgeDatas.map(findBast(_))
  }


}
