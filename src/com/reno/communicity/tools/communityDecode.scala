package com.reno.communicity.tools

/**
 * Created by reno on 2015/10/28.
 */
object communityDecode {
  /**
   * 社区检测解码
   * @param g
   * @return
   */
  def decode(g:Array[Int]):Array[Int]={
    var current_cluster=0
    val len=g.length
    var neighbour=(-1)
    val cluster_assign=Array.fill(len){-1}
    for(i<- 0 to (len-1)){
      val previous=new scala.collection.mutable.Stack[Int]
      if(cluster_assign(i)==(-1)){
        cluster_assign(i)=current_cluster
        neighbour=g(i)
        previous.push(i)
        while(cluster_assign(neighbour)==(-1)){
          previous.push(neighbour)
          cluster_assign(neighbour)=current_cluster
          neighbour=g(neighbour)
        }
        if(cluster_assign(neighbour)!=current_cluster){
          while(!previous.isEmpty){
            cluster_assign(previous.pop)=cluster_assign(neighbour)
          }

        }else{
          current_cluster+=1
        }
      }
    }
    cluster_assign
  }
  /**
   * 测试
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val out=decode(Array(3,0,3,1,5,6,4,7,7,2,3,2,1,2,3,4,5,6,8,6,5,21,21,21,21))
    for(i<-out){
      print(i+" ")
    }
  }
}
