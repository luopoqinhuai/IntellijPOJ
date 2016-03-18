package com.reno.recommecdation.main

import com.reno.recommecdation.format.reno_particle
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by reno on 2015/10/12.
 */
object testmain {





  def joinT(one:Array[reno_particle]):Set[Int]={
    val arrs=one
    var list=List.empty[Int]
    for(a<-arrs){
     list= list++a.position.toList
    }
    list.toSet
  }

  def joinT2(one:Array[(Int,Double)]):Set[Int]={
    val arrs=one
    var list=List.empty[Int]
    for(a<-arrs){
      list =list++List(a._1)
    }
    list.toSet
  }




  /**
   * 评估
   * @param args
   */
  def main(args: Array[String]): Unit ={
    val pathtest="hdfs://master:54321/home/spark/recommend_system/ml-100k/u1.test"
    val splitCode="\t";

    val conf = new SparkConf().setAppName("recommend-test"+System.nanoTime())
    val sc = new SparkContext(conf)


    val test_tmp1=sc.textFile(pathtest)
    val rawRating_Test=test_tmp1.map(x=>{val arr= x.split(splitCode);Array(arr(0),arr(1),arr(2))})
    //testdata is RDD[Rating]
    val testdata=rawRating_Test.map{ case Array(user,item,score)=>(user.toInt,(item.toInt,score.toDouble))}
    val testgroup=testdata.groupByKey.map{case (id,iter)=>(id,iter.toArray)}
    val resOut=sc.objectFile[(Int, Array[reno_particle])]("hdfs://master:54321/home/spark/testlog/Allout1469639203654043")

    //RDD[(id,(Array[reno_particle],Array[(Int,Double)]))]
    val tobeTest=resOut.join(testgroup)

    tobeTest.map{
      case (id,(ta,ra))=>{
        val s1=joinT(ta)
        val s2=joinT2(ra)
        val together= s1 & s2
        (s1.size,s2.size,together.size)
      }
    }




  }
}
