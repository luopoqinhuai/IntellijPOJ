package com.reno.recommecdation.main

import java.io._
import com.reno.recommecdation.format.population
import com.reno.recommecdation.tools.RatingStatistics
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.{SparkConf, SparkContext}

/**
 * do  after getTopN
 *
 * Created by reno on 2015/10/28.
 */
object getOnePerson {
   def main (args: Array[String]) {
     val logpath="hdfs://master:54321/home/spark/testlog/"
     val pathtrain="hdfs://master:54321/home/spark/recommend_system/ml-100k/u1.base"
     val conf = new SparkConf().setAppName("getOnePerson"+System.nanoTime())
     val sc = new SparkContext(conf)
     sc.setCheckpointDir("/home/spark/checkpoint")
     val splitCode="\t"
     val train_tmp1=sc.textFile(pathtrain)
     val rawRating_Train=train_tmp1.map(x=>{val arr= x.split(splitCode);Array(arr(0),arr(1),arr(2))})
     val traindata=rawRating_Train.map{ case Array(user,item,score)=>Rating(user.toInt,item.toInt,score.toDouble)}
     /////从这里开始////从RDD取得一个用户优化/////////////////////////////////////
     // val path="hdfs://master:54321/home/spark/testlog/sortGroups_100"
     // val sortedGroups = sc.objectFile[(Int,Array[Rating])](path)
     // val tests=sortedGroups.take(100)
     // val testone=tests(1)
     ///////////////////////////////////////////////////////////////////////
     //val outObj = new ObjectOutputStream(new FileOutputStream("/home/spark/reno/oneperson.obj"))
     //从本地取得一个用户
     val in = new ObjectInputStream(new FileInputStream("/home/spark/reno/oneperson.obj"))
     val testone=in.readObject().asInstanceOf[(Int,Array[org.apache.spark.mllib.recommendation.Rating])]
     in.close()

     val number =10000
     val one = sc.parallelize(1 to number)

     val mStatistics=RatingStatistics.meanAndVariance(traindata)
     val popu=new population(testone,mStatistics)
     val populations=popu.initialization(number,one)
     val oneResult=popu.run(populations,50)
     val tmpnum=oneResult._1
     val mpath="/home/spark/reno/recom_"+System.nanoTime()+".txt"
     val writer = new PrintWriter(new File(mpath))
     for(i<- oneResult._2){
       val s=i.fitness(0)+","+i.fitness(1)+"\n"
       writer.write(s)
     }
     writer.close()
  }
}
