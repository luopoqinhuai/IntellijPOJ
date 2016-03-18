package com.reno.recommecdation.format

/**
 * Created by reno on 2015/10/13.
  */
  class reno_particle(val position:Array[Int]) extends  Serializable{
    var fitness:Array[Double]=Array.fill(2){0}


  def in(arr:Array[reno_particle]):Boolean={
    var flag=false
    val len=arr.length
    for(i<-0 to len-1 if(!flag)){
      if(this==arr(i)) flag=true
    }

    flag
  }

  def  !=(other:reno_particle):Boolean={
    val this_len=this.position.length
    val other_len=other.position.length
    var flag=false
    var defin=true
    if(this_len!=other_len){
      flag=true
    }
    else{
      for(i<-0 to this_len-1 if defin){
        if(this.position(i)!=other.position(i)){
          defin=false
        }
      }
      if(defin) flag=false
      else flag=true
    }
    flag
  }
  def  ==(other:reno_particle):Boolean={
    val this_len=this.position.length
    val other_len=other.position.length
    var flag=false
    var defin=true
    if(this_len!=other_len){
      flag=false
    }
    else{
      for(i<-0 to this_len-1 if defin){
        if(this.position(i)!=other.position(i)){
          defin=false
        }
      }
      if(defin) flag=true
      else flag=false
    }
    flag
  }

}
