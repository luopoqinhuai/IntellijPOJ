package com.reno.newTrain

import java.io.PrintWriter

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{RandomForest, GradientBoostedTrees}
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by reno on 2015/12/16.
 */




class change {


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

  val predict_x=sc.textFile("hdfs://master:54321/home/spark/reno/newtrain/test_x.csv",minPartitions = 100)
  val train_x=sc.textFile("hdfs://master:54321/home/spark/reno/newtrain/train_x.csv",minPartitions = 100)
  val train_y=sc.textFile("hdfs://master:54321/home/spark/reno/newtrain/train_y.csv",minPartitions = 100)

  val trainfeatures=train_x.map(x=>x.split(",")).map(x=>splitArray(x))
  val predictfeatures=predict_x.map(x=>x.split(",")).map(x=>splitArray(x)).cache()

  val trainlables=train_y.map{x=>val dt=x.split(",");(dt(0).toInt,dt(1).toInt)}

  //reload
 // val labledTraindatas:RDD[(Int,(Array[Double],Int))]=trainfeatures.join(trainlables)
  val labledTraindatas=sc.objectFile[(Int,(Array[Double],Int))]("/home/spark/reno/lbt.rdd")


  val traindata=labledTraindatas.map{
    case (uid,(features,label))=>
      val vec=Vectors.dense(features)
      val lb=LabeledPoint(label.toDouble,vec)
      (uid,lb)
  }.cache()

  val predictdata=predictfeatures.map{
    case (uid,features)=>
      val vec=Vectors.dense(features)
      (uid,vec)
  }.cache()

  predictdata.saveAsObjectFile("/home/spark/predict_rdd.rdd")





  val Pt=traindata.filter(x=>x._2.label==1.0).map(x=>x._2)
  val Nt=traindata.filter(x=>x._2.label==0.0).map(x=>x._2)
  val split_p=Pt.randomSplit(Array(0.75, 0.25),System.nanoTime())
  val split_n=Nt.randomSplit(Array(0.75, 0.25),System.nanoTime())
  val trainp=split_p(0)
  val testp=split_p(1)
  val trainn=split_n(0)
  val testn=split_n(1)

  val train_all= trainp union trainn
  train_all.saveAsObjectFile("/home/spark/reno/train_rdd.rdd")

  val test_all= testp union testn
  test_all.saveAsObjectFile("/home/spark/reno/test_rdd.rdd")

  //==========
  val numClasses = 2
  val categoricalFeaturesInfo = Map[Int, Int]()
  val numTrees = 500 // Use more in practice.
  val featureSubsetStrategy = "auto" // Let the algorithm choose.
  val impurity = "variance"
  val maxDepth = 10
  val maxBins = 32

  val model = RandomForest.trainRegressor(train_all, categoricalFeaturesInfo,
    numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)



  val output2=predictdata.map {
    case lb=>
      val prediction = model.predict(lb._2)
      (lb._1,prediction)
  }.cache()





  val outfile=new PrintWriter("/home/spark/reno/output_1217_500.txt")

  for(i<-output2.collect){
    outfile.write(i._1+","+i._2+"\n")
  }
  outfile.close()


  val t_s_3 =test_all.map {
    case lb=>
      val prediction = model.predict(lb.features)
      (lb.label,prediction)
  }.cache()
  t_s.count

  //===========




  /* val Nt=traindata.filter(x=>x._2.label==0.0).map(x=>x._2.features)      //negative features
   val N_matrix=new RowMatrix(Nt)

   val Pt=traindata.filter(x=>x._2.label==1.0).map(x=>x._2.features)      //positiove features
   val P_matrix=new RowMatrix(Pt)


   val PCA_traindata=labledTraindatas.map{
     case (uid,(features,label))=>
       val vec=Vectors.dense(features)
       vec
   }.cache()

   val  PCA_predictdata=predictfeatures.map{
     case (mkey,feature)=>
       val vec=Vectors.dense(feature)
       vec
   }.cache()
   val PAC_all=PCA_traindata union PCA_predictdata


   val PACmatrix=new RowMatrix(PAC_all)
   val PAC_pc=PACmatrix.computePrincipalComponents(50)




   //"/home/spark/reno/PAC_p.rdd"
   val PAC_positive=P_matrix.multiply(PAC_pc).rows.cache()
   //RDD[org.apache.spark.mllib.linalg.Vector]

   //"/home/spark/reno/PAC_n.rdd"
   val PAC_negative=N_matrix.multiply(PAC_pc).rows.cache()


   val splits_PCA_N= PAC_negative.randomSplit(Array(0.7, 0.3),System.nanoTime())
   val splits_PCA_P= PAC_positive.randomSplit(Array(0.7, 0.3),System.nanoTime())


   val train_1_pca_N=splits_PCA_N(0).map(x=>LabeledPoint(0.0,x))
   val test_1_pca_N=splits_PCA_N(1).map(x=>LabeledPoint(0.0,x))

   val train_1_pca_P=splits_PCA_P(0).map(x=>LabeledPoint(1.0,x))
   val test_1_pca_P=splits_PCA_P(1).map(x=>LabeledPoint(1.0,x))

   val train_pca_1=train_1_pca_N.union(train_1_pca_P).cache()
   val  test_pca_1 = test_1_pca_N.union(test_1_pca_P).cache()

   val model_pca_1 = GradientBoostedTrees.train(train_pca_1, boostingStrategy)

 */








  val t_s_pca =test_pca_1.map {
    case lb=>
      val prediction = model_pca_1.predict(lb.features)
        //predict(lb.features)
      (lb.label, prediction)
  }



  change2.AUC(t_s_pca)





  //reload
  //val traindata=sc.objectFile[(Int, org.apache.spark.mllib.regression.LabeledPoint)]("/home/spark/reno/traindata_1.rdd")

  //////正负样本
  val N_traindata=traindata.filter(x=>x._2.label==0.0)
  val P_traindata=traindata.filter(x=>x._2.label==1.0)


  val splits_N= N_traindata.randomSplit(Array(0.7, 0.3),System.nanoTime())
  val splits_P= P_traindata.randomSplit(Array(0.7, 0.3),System.nanoTime())

  val train_1_N=splits_N(0).map(x=>x._2)
  val test_1_N=splits_N(1)

  val train_1_P=splits_P(0).map(x=>x._2)
  val test_1_P=splits_P(1)

  val train_1=train_1_N.union(train_1_P)
  val  test_1 = test_1_N.union(test_1_P)

//reload
//val test_1=sc.objectFile[(Int, org.apache.spark.mllib.regression.LabeledPoint)]("hdfs://master:54321/home/spark/reno/test_1.rdd")
//val  train_1=sc.objectFile[org.apache.spark.mllib.regression.LabeledPoint]("hdfs://master:54321/home/spark/reno/train_1.rdd")

  val fttt=train_1.map(x=>x.features)
  val matrix=new RowMatrix(fttt)
  val pc=matrix.computePrincipalComponents(100)







  val boostingStrategy = BoostingStrategy.defaultParams("Regression")
  //boostingStrategy.numIterations = 10 // Note: Use more iterations in practice.
  //boostingStrategy.treeStrategy.maxDepth = 10
  //boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

  val model_1 = GradientBoostedTrees.train(train_1, boostingStrategy)
  model_1.save(sc,"/home/spark/reno/model_1.model")

  GradientBoostedTreesModel.load(sc,"/home/spark/reno/model_1.model")





  val t_s =test_1.map {
    case (mkey,lb)=>
      val prediction = model_1.predict(lb.features)
      (mkey, prediction)
  }
  t_s.count


  val  t_l = test_1.map(x=>(x._1,x._2.label))

  t_l.count



//=================================================================================



  /*val labelsAndPredictions = predictdata.map {
    case (mkey,feature)=>
      val prediction = model_1.predict(feature)
      (mkey, prediction)
  }

  val outtt=labelsAndPredictions.map{x=>var out=0; if(x._2<0.5) out=0 else out=1;(x._1,out)}


  val outfile=new PrintWriter("/home/spark/reno/test.txt")

  for(i<-labelsAndPredictions.collect){
    outfile.write(i+"\n")
  }
*/
}
