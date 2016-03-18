package com.reno.recommecdation.main

import com.reno.recommecdation.format._
import com.reno.recommecdation.tools.RatingStatistics
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by reno on 2015/10/20.
 */
object test {
  def main(args: Array[String]): Unit = {
    val pathtrain="hdfs://master:54321/home/spark/recommend_system/ml-100k/u1.base"
    val conf = new SparkConf().setAppName("recommend-test"+System.nanoTime())
    val sc = new SparkContext(conf)
    val splitCode="\t"
    val train_tmp1=sc.textFile(pathtrain)
    val rawRating_Train=train_tmp1.map(x=>{val arr= x.split(splitCode);Array(arr(0),arr(1),arr(2))})
    val traindata=rawRating_Train.map{ case Array(user,item,score)=>Rating(user.toInt,item.toInt,score.toDouble)}


    val mStatistics=RatingStatistics.meanAndVariance(traindata)

    /////从这里开始
    val path="hdfs://master:54321/home/spark/testlog/sortGroups_left"


    val sortedGroups = sc.objectFile[(Int,Array[Rating])](path)

    val logpath="hdfs://master:54321/home/spark/testlog/"
    //RDD[(Int,Array[reno_particle])]

    val number =1000
    val one = sc.parallelize(1 to number)


    /////////////////并行/////////////////////////////////////////
    val arrayPersonnal=sortedGroups.collect()
    val arrRes=arrayPersonnal.map{
      testone=>
        val popu=new population(testone,mStatistics)
        val populations=popu.initialization(number,one)
        popu.run(populations,10)
    }
    // RDD[(Int, Array[reno_particle])]
    sc.makeRDD(arrRes).saveAsObjectFile(logpath+"Allout"+System.nanoTime())
    ////////////////////////////////////////////////////////////////



    ///////////////单机//////////////////////////////////////
    //  val testone=sortedGroups.first
    //  val popu=new population(testone,mStatistics)
    //  val populations=popu.initialization(number,one)

    //   val res=popu.run(populations,10)
    //   sc.makeRDD(res).saveAsObjectFile(logpath+System.nanoTime())
    ///////////////////////////////////////////////////////////////////



    /*   val personnal=arrayPersonnal.map{testone=>
           val popu=new population(testone,mStatistics)
           val populations=popu.initialization(number,one)
           (testone._1,popu.run(populations,10))
       }*/

    /*    val RDDpersonnal=sc.parallelize(personnal)

        RDDpersonnal.saveAsObjectFile(logpath+"renoAll"+System.nanoTime())*/
    /*        val testone=sortedGroups.first

            val popu=new population(testone,mStatistics)
            val populations=popu.initialization(100,sc)
            println("\n\n\nstart now\n\n\n")

           // populations.collect.map(x=>println(x.fitness(0)+" "+x.fitness(1)))

           popu.run(populations,10)

        println("\n\n\nend now\n\n\n")*/

    /*    println("\n\n\nstart now\n\n\n")
        val one=sc.parallelize(1 to 50,5)
        val two=one.map{x=> new reno_particle(Array(1,2,3,4,5,6))}
        println("\n\n\nend now\n\n\n")*/


  }





}
