package com.reno.newTrain

import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest}
import org.apache.spark.mllib.tree.configuration.BoostingStrategy

/**
 * Created by reno on 2015/12/17.
 */
class main_2 {
  val sc = new SparkContext()

  val train_all= sc.objectFile[LabeledPoint]("/home/spark/reno/train_rdd.rdd").cache()
  val test_all = sc.objectFile[LabeledPoint]("/home/spark/reno/test_rdd.rdd").cache()
  val predictdata=sc.objectFile[(Int,org.apache.spark.mllib.linalg.Vector)]("/home/spark/predict_rdd.rdd").cache()
  train_all.count()
  test_all.count()
  predictdata.count()

  //==========RF======================
  val categoricalFeaturesInfo = Map[Int, Int]()
  val numTrees = 500 // Use more in practice.
  val featureSubsetStrategy = "auto" // Let the algorithm choose.
  val impurity = "variance"
  val maxDepth = 10
  val maxBins = 32
  val model_rf = RandomForest.trainRegressor(train_all, categoricalFeaturesInfo,
    numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
  //================================

  val boostingStrategy = BoostingStrategy.defaultParams("Regression")
  boostingStrategy.setNumIterations(10)
  boostingStrategy.treeStrategy.setMaxDepth(20)


 //   boostingStrategy.treeStrategy.categoricalFeaturesInfo=Map[Int,Int]()
  val model_dt = GradientBoostedTrees.train(train_all, boostingStrategy)





}
