package com.reno.newTrain

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.{RandomForestModel, GradientBoostedTreesModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
/**
 * Created by reno on 2015/12/16.
 */
object change2 {

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


  def AUC(data:RDD[(Double,Double)]):Double={
    val pp=data.filter(x=>x._1==1.0).map(x=>x._2).cache()
    val nn=data.filter(x=>x._1==0.0).map(x=>x._2).cache()
    val numP=pp.count()
    val numN=nn.count()
    val dkr=pp.cartesian(nn).map{
      case (p,n)=>
        var mscore=0.0
        if(p>n) mscore=1.0
        else if (p==n) mscore=0.5
        else mscore=0.0
        mscore
    }.reduce(_+_)
    pp.unpersist()
    nn.unpersist()
    dkr/(numN*numP)
  }

  def testTest(testdata:RDD[LabeledPoint],model:GradientBoostedTreesModel):Double={
    val t_s =testdata.map {
      case lb=>
        val prediction = model.predict(lb.features)
        (lb.label,prediction)
    }.cache()
    t_s.count
    val auc=AUC(t_s)
    t_s.unpersist()
    auc
  }

  def testTest(testdata:RDD[LabeledPoint],model:RandomForestModel):Double={
    val t_s =testdata.map {
      case lb=>
        val prediction = model.predict(lb.features)
        (lb.label,prediction)
    }.cache()
    t_s.count
    val auc=AUC(t_s)
    t_s.unpersist()
    auc
  }

  //


  def AUC(RDDlabel:RDD[(Int,Double)],RDDscore:RDD[(Int,Double)]):Double={
    RDDlabel.cache()
    RDDlabel.count()
    RDDscore.cache()
    RDDscore.count()
    val t_s_l=RDDscore.join(RDDlabel)
    val t_s_l_N=t_s_l.filter(x=>x._2._2==0.0).cache()
    val t_s_l_P=t_s_l.filter(x=>x._2._2==1.0).cache()
    val num_N=t_s_l_N.count
    val num_P=t_s_l_P.count
    val zhongji=t_s_l_P.cartesian(t_s_l_N)
    val SSS=zhongji.map{
      x=>
        var mscore=0.0
        if(x._1._2._1>x._2._2._1) {
          mscore = 1.0
        }else if(x._1._2._1==x._2._2._1){
          mscore=0.5
        }else{
          mscore=0.0
        }
        mscore
    }.reduce(_+_)
    RDDlabel.unpersist()
    RDDscore.unpersist()
    t_s_l_N.unpersist()
    t_s_l_P.unpersist()
    SSS/(num_N*num_P)
  }





  def splitArray(arr:Array[String])={
    val len=arr.length-1
    val outtarr=new Array[Double](len)
    val uid=arr(0).toInt
    for(i<-0 to len-1){
      outtarr(i)=arr(i+1).toDouble
    }
    (uid,outtarr)
  }


  /**
   *最终预测数据
   * @param predictfeatures
   * @param model
   * @return
   */
  def finalpredict(predictfeatures:RDD[(Int,Array[Double])],model:GradientBoostedTreesModel):RDD[(Int,Double)]={
    val predictdata=predictfeatures.map{
      case (uid,features)=>
        val vec=Vectors.dense(features)
        (uid,vec)
    }
    predictdata.map {
      case (mkey,lb)=>
        val prediction = model.predict(lb)
        (mkey, prediction)
    }

  }


}
