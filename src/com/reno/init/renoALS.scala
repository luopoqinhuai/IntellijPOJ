/**
 * Created by reno on 2015/9/23.
 */
package reno.init

// import com.reno.format.population

import com.reno.format.population
import com.reno.tools.RatingStatistics
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object renoALS{
  /**
   * @param rating    test data  Rating(user,product,score)
   * @param model     trained model
   * @return          ratingAndPrediction     ((user,product),(actualSCORE,predictedSOCRE))
   */
  def getPredicted(rating:RDD[Rating],model:MatrixFactorizationModel):RDD[((Int,Int),(Double,Double))] ={
    val usersProducts =rating.map{case Rating(user,product,score)=>(user,product)}
    val predictions =model.predict(usersProducts).map{case Rating(user,product,score)=> ((user,product),score)}
    val ratingAndPrediction = rating.map{case Rating(user,product,score)=>((user,product),score)}.join(predictions)
    ratingAndPrediction
  }

  /**
   * @param ratingAndPrediction      (user,product),(actualSCORE,predictedSOCRE))
   * @return                           RMSE
   */
  def getRMSE(ratingAndPrediction:RDD[((Int,Int),(Double,Double))]): Double ={
    val MSE= ratingAndPrediction.map{ case ((x1,x2),(actual,predicted)) => math.pow((actual-predicted),2)}.reduce(_+_) / ratingAndPrediction.count
    val RMSE = math.sqrt(MSE)
    RMSE
  }



  {
    val TestData=Array(
      (1,56),(3,23),(7,2),(76,1),(1,10),(4,3),(3,2),(5,1),(12,6)
    )
  }

  /**
   * 被支配 返回false
   * @param a
   * @param lb
   * @return
   */
  def IsZhiPei(a:(Int,Int),lb:List[(Int,Int)]):Boolean={
    var out=true
    for (b<- lb if out){
      println("---")
      if(b._1>a._1&&b._2>a._2 ) {
        out= false
      }
    }
    //print(a+"---")
    out
  }

  /**
   * 也可以用于更新外部种群。。。
   * @param la
   * @param lb
   * @return
   */
  def getParetofromList(la:List[(Int,Int)],lb:List[(Int,Int)]):List[(Int,Int)]={
    val la_left=la.filter(x=>IsZhiPei(x,lb))
    val lb_left=lb.filter(x=>IsZhiPei(x,la))
    la_left++lb_left.toSet.toList
  }

  /**
   * 得到非支配解
   * @param datas
   * @return
   */
  def getPareto(datas:RDD[(Int,Int)]): List[(Int,Int)] ={
    datas.map(x=>List(x)).reduce(getParetofromList(_,_))
  }





  def main(args: Array[String]): Unit = {
    val rank:Int=200
    val iteration:Int=10
    val lambda:Double = 0.01
    val num_k = 50    //每个用户取得前30个
    val mlogpath="hdfs://master:54321/home/spark/testlog/Allout/"
    //val pathtrain="hdfs://master:54321/home/spark/recommend_system/ml-10M100K/r1.train"
    //val pathtest="hdfs://master:54321/home/spark/recommend_system/ml-10M100K/r1.test"
    val pathtrain="hdfs://master:54321/home/spark/recommend_system/ml-100k/u1.base"
    val pathtest="hdfs://master:54321/home/spark/recommend_system/ml-100k/u1.test"
    val modelSavePath="hdfs://master:54321/home/spark/models/ALS_100k_u1"

    //val splitCode="::";
    val splitCode="\t";
    val conf = new SparkConf().setAppName("renoALS_test_100k")
    val sc = new SparkContext(conf)

    val train_tmp1=sc.textFile(pathtrain)
    val rawRating_Train=train_tmp1.map(x=>{val arr= x.split(splitCode);Array(arr(0),arr(1),arr(2))})
    //traindata is RDD[Rating]  just for ALS.train(data,)
    val traindata=rawRating_Train.map{ case Array(user,item,score)=>Rating(user.toInt,item.toInt,score.toDouble)}


    val mStatistics=RatingStatistics.meanAndVariance(traindata)

    //train model
    val model =ALS.train(traindata,rank,iteration,lambda)
    model.save(sc,modelSavePath)


    /**
     * 取得用户未评分的物品的集合
     */
    val usersRDD=model.userFeatures.map(x=>x._1)
    val productsRDD=model.productFeatures.map(x=>x._1)
    //(key,List(items))
    val akv=usersRDD.cartesian(productsRDD).groupByKey.map{case (key,items)=>(key,items.toList)}
    val okv=traindata.map{case Rating(user,item,score)=>(user,item)}.groupByKey.map{case (key,items)=>(key,items.toList)}
    val ajoin=akv.join(okv)
    val aleft=ajoin.map{case (id,(allkv,otherkv))=> (id ,allkv.diff(otherkv))}
    val tobepredict =aleft.flatMap{case (id,mList)=> mList.map(x=>(id,x))}
    //Rating(115,540,2.1600661351159482)

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val allpredict=model.predict(tobepredict).map{case Rating(user,item,score)=>(user,(item,score))}  //所有的笛卡尔积  user item 对的预测值   Rating(user,item score)
    val groups = allpredict.groupByKey()     //(userID,Iterator((itemID,score)))
    /**
     * sortedGroups (User ,List(item,score))   top 30 在此基础之上进行多目标优化
     */
    val sortedGroups=groups.map{x=>(x._1,x._2.toList.sortWith(_._2>_._2).take(num_k))}.map{ //(userID,List((itemID,score))) 排序好了
      case (user,arr)=>
        val out_arr=arr.map{
          case (item,score)=>
            Rating(user,item,score)
        }
        (user,out_arr.toArray)  //(userID,Array(Rating(u,i,s))) 排序好了
    }
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //val sortedGroups = model.recommendProductsForUsers(num_k)

    //RDD[(Int,Array[reno_particle])]
    val personnal=sortedGroups.map{testone=>
      val popu=new population(testone,mStatistics)
      // val populations=popu.initialization(100,sc)
      // (testone._1,popu.run(populations,10))
      null
    }
    personnal.saveAsObjectFile(mlogpath+System.nanoTime())

    /*    val personnal=sortedGroups.map{
          x=>
              x._2.reduce{
                (a,b) =>Map(a.product,mStatistics(a.product))++Map(b.product,mStatistics(b.product))
              }
           val popu=new population(sc,x,personnal)


        }*/

    /*
        val test_tmp1=sc.textFile(pathtest)
        val rawRating_Test=test_tmp1.map(x=>{val arr= x.split(splitCode);Array(arr(0),arr(1),arr(2))})
        //testdata is RDD[Rating]
        val testdata=rawRating_Test.map{ case Array(user,item,score)=>Rating(user.toInt,item.toInt,score.toDouble)}

        val ratingAndPrediction = getPredicted(testdata,model)
        val RMSE = getRMSE(ratingAndPrediction)
        println(RMSE)*/

  }
}