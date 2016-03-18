package com.reno.ceshi

import org.apache.spark.rdd.RDD
/**
 * Created by reno on 2015/10/20.
 */
class ceshi extends Serializable{
  def one(num:Int):Int={
    num+1
  }
  def two(data:RDD[Int]):RDD[Int]={
    data.map(one(_))
  }

}
