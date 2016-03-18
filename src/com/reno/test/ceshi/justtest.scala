package com.reno.test.ceshi

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by reno on 2015/10/30.
 */
object justtest {
  val sc=new SparkContext()
  var mparse=sc.makeRDD(1 to 1000,20)
  val num =mparse.count
  println("\n\n\n"+num+"\n\n\n")


def test(mparse:RDD[Int])={
  val out=mparse.mapPartitions(iter => {
    val childs = iter.toArray
    val mRand:java.util.Random=  new java.util.Random(System.nanoTime())
    val len=childs.length

    val newchilds=Array.fill(len){0}
    for(i<-0 to len-1){
      newchilds(i)=childs(mRand.nextInt(len))
    }
    newchilds.toIterator
  })
  out
  }

def cycle(parse:RDD[Int])={
  var mparse=parse
  for(i<-1 to 100){
    mparse=test(mparse)
    mparse.map(x=>Iterator(x)).count
  }

}

}
