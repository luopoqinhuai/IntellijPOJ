/**
 * Created by reno on 2015/9/19.
 */
package  reno.format
class BaseParticle(var poisition:Array[Int],var speed:Array[Int]){
  var pBest_poisition:Array[Int]=null
  var pBest_speed:Array[Int]    =null
  var gBest_poisition:Array[Int]=null
  var gBest_speed:Array[Int]    =null
  var ALL_Fitness:Array[Float]  =null

  /**
   * 入口参数为  Array[Int]   返回值为所有适应度的Array
   * @param f
   */
  def getFitness(f:(Array[Int])=>Array[Float]): Unit ={
    ALL_Fitness=f(poisition)
  }
}
