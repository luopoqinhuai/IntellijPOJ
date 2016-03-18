package com.reno.recommecdation.main

import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by reno on 2015/10/28.
 */
object  getTopN {
  def main(args: Array[String]): Unit = {
    val num_k=100
    val mlogpath="hdfs://master:54321/home/spark/testlog/sortGroups_100"
    val pathtrain="hdfs://master:54321/home/spark/recommend_system/ml-100k/u1.base"
    val modelSavePath="hdfs://master:54321/home/spark/models/ALS_100k_u1"
    val splitCode="\t";
    val conf = new SparkConf().setAppName("renoALS_test_100k")
    val sc = new SparkContext(conf)
    val train_tmp1=sc.textFile(pathtrain)
    val rawRating_Train=train_tmp1.map(x=>{val arr= x.split(splitCode);Array(arr(0),arr(1),arr(2))})
    val traindata=rawRating_Train.map{ case Array(user,item,score)=>Rating(user.toInt,item.toInt,score.toDouble)}

    val model =MatrixFactorizationModel.load(sc,modelSavePath)
    val usersRDD=model.userFeatures.map(x=>x._1)
    val productsRDD=model.productFeatures.map(x=>x._1)
    val akv=usersRDD.cartesian(productsRDD).groupByKey.map{case (key,items)=>(key,items.toList)}
    val okv=traindata.map{case Rating(user,item,score)=>(user,item)}.groupByKey.map{case (key,items)=>(key,items.toList)}
    val ajoin=akv.join(okv)
    val aleft=ajoin.map{case (id,(allkv,otherkv))=> (id ,allkv.diff(otherkv))}
    val tobepredict =aleft.flatMap{case (id,mList)=> mList.map(x=>(id,x))}
    val allpredict=model.predict(tobepredict).map{case Rating(user,item,score)=>(user,(item,score))}  //所有的笛卡尔积  user item 对的预测值   Rating(user,item score)
    val groups = allpredict.groupByKey()     //(userID,Iterator((itemID,score)))
    val sortedGroups=groups.map{x=>(x._1,x._2.toList.sortWith(_._2>_._2).take(num_k))}.map{ //(userID,List((itemID,score))) 排序好了
      case (user,arr)=>
        val out_arr=arr.map{
          case (item,score)=>
            Rating(user,item,score)
        }
        (user,out_arr.toArray)
    }
    sortedGroups.saveAsObjectFile(mlogpath)
  }

}
