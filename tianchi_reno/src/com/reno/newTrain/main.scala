package com.reno.newTrain

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by reno on 2015/12/16.
 */
object main {

  def splitArray(arr:Array[String])={
    val len=arr.length-1
    val outtarr=new Array[Double](len)
    val uid=arr(0).toInt
    for(i<-0 to len-1){
      outtarr(i)=arr(i+1).toDouble
    }
    (uid,outtarr)
  }

  val conf = new SparkConf().setAppName("Tianchi-gd")
  val sc = new SparkContext(conf)

  val predictfeatures:RDD[(Int,Array[Double])]=sc.textFile("hdfs://master:54321/home/spark/reno/newtrain/test_x.csv",minPartitions = 100).
        map(x=>x.split(",")).map(x=>splitArray(x))

  val labledTraindatas=sc.objectFile[(Int,(Array[Double],Int))]("/home/spark/reno/lbt.rdd")

  val traindata=labledTraindatas.map{
    case (uid,(features,label))=>
      val vec=Vectors.dense(features)
      val lb=LabeledPoint(label.toDouble,vec)
      lb
  }.cache()

  val splitN=traindata.filter(x=>x.label==0.0).randomSplit(Array(0.75,0.25),System.nanoTime())  //negative features
  val splitP=traindata.filter(x=>x.label==1.0).randomSplit(Array(0.75,0.25),System.nanoTime()) //positiove features
  val train_1_N=splitN(0)
  val test_1_N=splitN(1)
  val train_1_P=splitP(0)
  val test_1_P=splitP(1)
  val train_1=train_1_N.union(train_1_P).cache()
  val test_1 = test_1_N.union(test_1_P).cache()

/*

  val boostingStrategy = BoostingStrategy.defaultParams("Regression")
  boostingStrategy.setNumIterations(5)  // Note: Use more iterations in practice.\
  boostingStrategy.treeStrategy.setMaxDepth(20)
  boostingStrategy.treeStrategy.categoricalFeaturesInfo=Map[Int, Int]()


  val model_1 = GradientBoostedTrees.train(train_1, boostingStrategy)
*/



  val numClasses = 2
  val categoricalFeaturesInfo = Map[Int, Int]()
  val numTrees = 100 // Use more in practice.
  val featureSubsetStrategy = "auto" // Let the algorithm choose.
  val impurity = "variance"
  val maxDepth = 10
  val maxBins = 32


  val model_1 = RandomForest.trainClassifier(train_1, numClasses, categoricalFeaturesInfo,
    numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)


  change2.testTest(train_1,model_1)

  val t_s =test_1.map {
    case lb=>
      val prediction = model_1.predict(lb.features)
      (lb.label,prediction)
  }.cache()
  t_s.count





}
