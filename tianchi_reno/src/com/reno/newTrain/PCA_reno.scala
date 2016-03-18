package com.reno.newTrain

import java.io.PrintWriter

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD

/**
 * Created by reno on 2015/12/17.
 */
class PCA_reno {

  def AUClocal(data:RDD[(Double,Double)]):Double={
    val pp=data.filter(x=>x._1==1.0).map(x=>x._2).cache()
    val nn=data.filter(x=>x._1==0.0).map(x=>x._2).cache()
    val arrP=pp.collect()
    val arrN=nn.collect()
    var value_sum=0.0
    for(one<-arrP)
      for(two<-arrN){
        if(one>two){
          value_sum+=1.0
        }else if(one==two){
          value_sum+=0.5
        }
      }
    value_sum/(arrP.size*arrN.size)
  }

  val sc = new SparkContext()

  val train_all= sc.objectFile[LabeledPoint]("/home/spark/reno/train_rdd.rdd").cache()
  val test_all = sc.objectFile[LabeledPoint]("/home/spark/reno/test_rdd.rdd").cache()
  val predictdata=sc.objectFile[(Int,org.apache.spark.mllib.linalg.Vector)]("/home/spark/predict_rdd.rdd").cache()
  train_all.count()
  test_all.count()
  predictdata.count()

/////////////////////////////////////////////////////////////////////////////////
  val part1=train_all.map(x=>x.features)                                      //
  part1.count                                                                 //
  val part2=test_all.map(x=>x.features)                                       //
  part2.count                                                                 //
  val part3=predictdata.map(x=>x._2)                                          //
  part3.count                                                                 //
  //val allfeatures=(part1 union part2 union part3).cache()                     //
 // allfeatures.saveAsObjectFile("/home/spark/reno/allfeatures_rdd.rdd")
 //
  val allfeatures= sc.objectFile[org.apache.spark.mllib.linalg.Vector]("/home/spark/reno/allfeatures_rdd.rdd")
////////////////////////////////////////////////////////////////////////////////






  val PCAmatrix=new RowMatrix(allfeatures)
  val PCA_pc=PCAmatrix.computePrincipalComponents(500)

  val  Nt=train_all.filter(x=>x.label==0.0).map(x=>x.features)
  val N_matrix=new RowMatrix(Nt)
  N_matrix.numCols()
  val Pt=train_all.filter(x=>x.label==1.0).map(x=>x.features)
  val P_matrix=new RowMatrix(Pt)
  P_matrix.numCols()
  val PCA_positive=P_matrix.multiply(PCA_pc).rows.cache()
  val PCA_negative=N_matrix.multiply(PCA_pc).rows.cache()
  PCA_positive.count
  PCA_negative.count

  val splits_PCA_N= PCA_negative.randomSplit(Array(0.75, 0.25),System.nanoTime())
  val splits_PCA_P= PCA_positive.randomSplit(Array(0.75, 0.25),System.nanoTime())
  val train_1_pca_N=splits_PCA_N(0).map(x=>LabeledPoint(0.0,x))
  val test_1_pca_N=splits_PCA_N(1).map(x=>LabeledPoint(0.0,x))
  val train_1_pca_P=splits_PCA_P(0).map(x=>LabeledPoint(1.0,x))
  val test_1_pca_P=splits_PCA_P(1).map(x=>LabeledPoint(1.0,x))

  val train_pca_1=train_1_pca_N.union(train_1_pca_P).cache()
  val test_pca_1 = test_1_pca_N.union(test_1_pca_P).cache()

  train_pca_1.count
  train_pca_1.saveAsObjectFile("/home/spark/reno/train_pca_rdd_500.rdd")

  test_pca_1.count

  test_pca_1.saveAsObjectFile("/home/spark/reno/test_pca_rdd_500.rdd")



  //=============RF=====================================
  val categoricalFeaturesInfo = Map[Int, Int]()
  val numTrees = 500 // Use more in practice.
  val featureSubsetStrategy = "auto" // Let the algorithm choose.
  val impurity = "variance"
  val maxDepth = 8
  val maxBins = 10

  val model_3 = RandomForest.trainRegressor(train_pca_1, categoricalFeaturesInfo,
    numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

  model_3.save(sc,"/home/spark/reno/rf500_model_f_500.model")

  val sameModel = RandomForestModel.load(sc, "/home/spark/reno/rf500_model_f_500.model")

  //===============  ===================================



  def updatesome(T_D:RDD[(Double,Double)],splitdata:Double):Double={
    T_D.map{
      case (lb,pre)=>
        var score=0
        if (pre>splitdata&& lb==1.0){
          score=1
        }
        if(pre<=splitdata&& lb==0.0){
          score=1
        }
        score
    }.reduce(_+_)
  }


 val t_s =test_all.map {
    case lb=>
      val prediction = sameModel.predict(lb.features)
      (lb.label,prediction)
  }.cache()

  AUClocal(t_s)

  val res9=predictdata.map{
    case (id,lb)=>
      val prediction = model_3.predict(lb)
      (id,prediction)
  }.cache()




  val outfile=new PrintWriter("/home/spark/reno/ceshi001.txt")

  for(i<-res9.collect){
    outfile.write(i._1+","+i._2+"\n")
  }
  outfile.close()





  val indexPredictData=predictdata.map(x=>new IndexedRow(x._1.toLong,x._2))
  val indexMatrix=new IndexedRowMatrix(indexPredictData)
  //val PCApredict:RDD[IndexedRow]=indexMatrix.multiply(PCA_pc).rows







}
