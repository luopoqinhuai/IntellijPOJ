package com.reno.recommecdation.main

import com.reno.recommecdation.format.population
import com.reno.recommecdation.tools.RatingStatistics
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by reno on 2015/11/9.
 */


object recommendation{


  /**
   * 入口参数 ->  训练集 分割符 rank,iteration,lambda,num_k,种群大小
   * @param args
   */
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("recommendation-Main")
    val sc = new SparkContext(conf)
    val pathtrain=args(0)
    val splitCode=args(1);
    val rawRating_Train=sc.textFile(pathtrain).map(x=>{val arr= x.split(splitCode);Array(arr(0),arr(1),arr(2))})
    val traindata=rawRating_Train.map{ case Array(user,item,score)=>Rating(user.toInt,item.toInt,score.toDouble)}

    val mStatistics=RatingStatistics.meanAndVariance(traindata)
    val rank=args(2).toInt             //val rank:Int=200
    val iteration=args(3).toInt        //val iteration:Int=10
    val lambda=args(4).toDouble        //val lambda:Double = 0.01

    val model =ALS.train(traindata,rank,iteration,lambda)

    val modelSavePath="hdfs://master:54321/home/spark/models/ALS_10M100k_r1"

    model.save(sc,modelSavePath)


    val num_k=args(5).toInt            //val num_k:Int=100
    val usersRDD=model.userFeatures.map(x=>x._1)
    val productsRDD=model.productFeatures.map(x=>x._1)
    val akv=usersRDD.cartesian(productsRDD).groupByKey.map{case (key,items)=>(key,items.toList)}
    val okv=traindata.map{case Rating(user,item,score)=>(user,item)}.groupByKey.map{case (key,items)=>(key,items.toList)}
    val ajoin=akv.join(okv)
    val aleft=ajoin.map{case (id,(allkv,otherkv))=> (id ,allkv.diff(otherkv))}
    val tobepredict =aleft.flatMap{case (id,mList)=> mList.map(x=>(id,x))}
    val allpredict=model.predict(tobepredict).map{case Rating(user,item,score)=>(user,(item,score))}
    val groups = allpredict.groupByKey()     //(userID,Iterator((itemID,score)))
    val sortedGroups=groups.map{x=>(x._1,x._2.toList.sortWith(_._2>_._2).take(num_k))}.map{ //(userID,List((itemID,score))) 排序好了
        case (user,arr)=>
          val out_arr=arr.map{
            case (item,score)=>
              Rating(user,item,score)
          }
          (user,out_arr.toArray)
      }

    val number =args(6).toInt
    val one = sc.parallelize(1 to number)

    val logpath="hdfs://master:54321/home/spark/testlog/"
    val arrayPersonnal=sortedGroups.collect()
    val arrRes=arrayPersonnal.map{
      testone=>
        val popu=new population(testone,mStatistics)
        val populations=popu.initialization(number,one)
        popu.run(populations,30)
    }
    // RDD[(Int, Array[reno_particle])]
    sc.makeRDD(arrRes).saveAsObjectFile(logpath+"RECOMMENDATION"+System.nanoTime())



  }



}

