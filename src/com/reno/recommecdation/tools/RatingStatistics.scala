package com.reno.recommecdation.tools

import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

/**
 * Created by reno on 2015/10/15.
 */
object RatingStatistics {

  /**
   * private function for
   * cumpute the mean of Array x
   * @param x
   * @return
   */
  private def mean(x: Array[Double]): Double = {
    x.sum * 1.0 / x.length
  }

  /**
   * private function for
   * calculate the standard variance of Array x
   * @param x
   * @param mean
   * @return
   */
  private def Variance(x: Array[Double], mean: Double): Double = {
    val length = x.length
    //方差
    val variance = x.map(i => (i - mean) * (i - mean) / length).sum
    //标准差
    math.sqrt(variance)
  }

  /**
   * get the mean and standard variance for ratings:RDD[Rating]
   * f2要求统计的是: 所有用户 对该物品的 评分
   * @param ratings
   * @return
   */
  def meanAndVariance(ratings: RDD[Rating]): Map[Int, (Double, Double)] = {
    //productRatings: Array[(Int, Iterable[Double])]  (productid,Iter(ratings))
    val productRatings = ratings.map(x => (x.product, x.rating)).groupByKey().collect()
    productRatings.map(
      x => {
        val x2 = x._2.toArray
        val m = this.mean(x2)
        (x._1 ->(m, this.Variance(x2, m)))
      }).toMap
  }
}
