package com.reno.communicity.format

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Created by reno on 2015/11/23.
 */
class rdd_particle(var position:RDD[(Long,Int)],var speed:RDD[(Long,Int)]) {
  var Pg_p:RDD[(Long,Int)]=position.map(x=>x).setName("Pg_p0").persist(StorageLevel.MEMORY_AND_DISK)
  Pg_p.foreach(x => {})
  var Pp_p:RDD[(Long,Int)]=position.map(x=>x).setName("Pp_p0").persist(StorageLevel.MEMORY_AND_DISK)
  Pp_p.foreach(x => {})
  var gFitness:Double=Double.MaxValue
  var pFitness:Double=Double.MaxValue
  var Fitness :Double=Double.MaxValue
  var Nbest:RDD[(Long,Int)]=null
}
