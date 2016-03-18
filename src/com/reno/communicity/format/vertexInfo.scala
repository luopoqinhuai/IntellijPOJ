package com.reno.communicity.format

/**
 * Created by reno on 2015/12/22.
 */
class vertexInfo extends Serializable{
  var degree: Int = 0                       // 该节点度值
  var community: Long = 0                   // 该节点所属社区
  var communityDegreeSum: Long = -1         // 该社区的度数之和   Sigma tot

  var neighDegree: Int = 0                  // 目标节点的度值
  var neighCommunity: Long = -1             // 目标节点所属社区
  var neighCommunityDegreeSum: Long = -1    // 目标节点的社区总权重
  var edgeCount: Long = -1                  // 该节点与目标节点的连线条数

}
