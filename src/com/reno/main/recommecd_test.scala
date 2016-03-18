package com.reno.main

import com.reno.format.reno_particle
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Renoook on 2015/10/26.
 */
object recommecd_test {

  def main(args: Array[String]): Unit = {
    val pathtest="hdfs://master:54321/home/spark/recommend_system/ml-100k/u1.test"
    val conf = new SparkConf().setAppName("recommend-test" + System.nanoTime())
    val sc = new SparkContext(conf)

    val splitCode="\t"
    val test_tmp1=sc.textFile(pathtest)
    val rawRating_Test=test_tmp1.map(x=>{val arr= x.split(splitCode);Array(arr(0),arr(1),arr(2))})
    val testdata=rawRating_Test.map{ case Array(user,item,score)=>Rating(user.toInt,item.toInt,score.toDouble)}

    val mpath="hdfs://master:54321/home/spark/testlog/Allout1399482381063279"
    val recommendData=sc.objectFile[(Int, Array[reno_particle])](mpath)

  }

}
