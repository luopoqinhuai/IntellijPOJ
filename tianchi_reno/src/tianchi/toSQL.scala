/*
package tianchi

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by reno on 2015/11/9.
 */
//使用地点place   公交线路    设备ID    用户公交卡ID     公交卡办理地点     YYYYMMDD    hh  week  卡类型
//case class mLineData(usePlace: Int, line:Int,deviceID:Int,userID:Int,cardPlace:Int,mdate:String,hour:Int,week:Int,cardType:Int)

//时间转化成整数   20140820   ---  0
case class mLineData2(usePlace: Int, line:Int,deviceID:Int,userID:Int,cardPlace:Int,mdate:Int,hour:Int,week:Int,cardType:Int)



object  toSQL {
  def ArraytoFile(mpath:String,datas:Array[(Int,Int)])={
    val writer = new PrintWriter(new File(mpath))
    for(i<- datas){
      val s=i._1+"\t"+i._2+"\n"
      writer.write(s)
    }
    writer.close()
  }

  def parseDate(mdate:String):Date={
    val format = new SimpleDateFormat("yyyyMMdd")
    format.parse(mdate)
  }

  def DatetoInt(astr:String,bstr:String):Int={
    val A=parseDate(astr)
    val B=parseDate(bstr)
    val out=(A.getTime-B.getTime)/(24 * 60 * 60 * 1000)
    out.toInt
  }

  def IntDayToweek(day:Int,mB:String="20140820")={
    val B=parseDate(mB)
    val lll=day*(24*60*60*1000)+B.getTime
    new Date(lll)
  }




  def main(args: Array[String]) {
    val outputpath="/home/spark/reno/tianchi/"
    val conf = new SparkConf().setAppName("Tianchi-gd")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val datas = sc.textFile("/home/spark/gd/gd_train_data_format2.txt").map(_.split("\t"))
    val starttime="20140821"    //friday
    val dbdatas = datas.map{p=>
      val md=DatetoInt(p(5),"20140821")
      mLineData2(p(0).toInt, p(1).toInt, p(2).toInt, p(3).toInt, p(4).toInt, md, p(6).toInt, p(7).toInt, p(8).toInt)
    }



    //val newdatas=dbdatas
    //20140825 周一开始
    //val newdatas=dbdatas.filter(x=>x.mdate>4&&x.mdate<131)         //08 25 - 12 28

      //周内  val newdatas =dbdatas.filter(x=>x.mdate>4&&x.mdate<131&& x.week<5 )
      //      val newdatas =dbdatas.filter(x=>x.mdate>4&&x.mdate<131&& x.week>4 )
    //  （第几周，数据）  0开始
    //val oneday=newdatas.map(x=>(x.mdate,x))
    //val stuoneday=oneday.filter(x=>x._2.cardType==2)
    //val out=stuoneday.groupByKey.map(x=>(x._1,x._2.size)).collect
    //ArraytoFile(outputpath+"onedayof.txt",out)



    //val oneweek=newdatas.map(x=>((x.mdate-5)/7,x))


    /**
     * start
     */

    val dfspath = "hdfs://master:54321/home/spark/reno/"
    //学生卡  看下是否相对稳定的出行量
    //(第几周, 数据)
    val stu=dbdatas.filter(x=>x.cardType==2).map{
      ml=>
        (ml.mdate/7,ml)
    }


    //stu 存储到hdfs    sc.objectFile[(Int,mLineData2)]("hdfs://master:54321/home/spark/reno/stu")


    val trainone= stu.filter(x=>x._1>5 && x._1<16)
    //key (第几周，第几小时，周几)  label 人数
    val mmmp=trainone.map(x=>((x._1,x._2.hour,x._2.week),1)).reduceByKey(_+_).collect.toMap

    val mmmpBd=sc.broadcast(mmmp)

    var LL=List.empty[(Int,Int,Int)]
    for (i<-10 to 15)
      for(j<-6 to 21)
        for(k<-0 to 6){
          LL=LL++List((i,j,k))
        }
    val alll=sc.makeRDD(LL)              //train range  week9-week15   test week16

    val Train1= alll.map{
      mkey=>
        val last1week=mmmpBd.value.getOrElse((mkey._1-1,mkey._2,mkey._3),0)
        val last2week=mmmpBd.value.getOrElse((mkey._1-2,mkey._2,mkey._3),0)
        val last3week=mmmpBd.value.getOrElse((mkey._1-3,mkey._2,mkey._3),0)
        val last4week=mmmpBd.value.getOrElse((mkey._1-4,mkey._2,mkey._3),0)

        val  last1week1hourB = mmmpBd.value.getOrElse((mkey._1 - 1, mkey._2 - 1, mkey._3), 0)
        val  last2week1hourB = mmmpBd.value.getOrElse((mkey._1 - 2, mkey._2 - 1, mkey._3), 0)
        val  last3week1hourB = mmmpBd.value.getOrElse((mkey._1 - 3, mkey._2 - 1, mkey._3), 0)
        val  last4week1hourB = mmmpBd.value.getOrElse((mkey._1 - 4, mkey._2 - 1, mkey._3), 0)

        val  last1week1hourA = mmmpBd.value.getOrElse((mkey._1 - 1, mkey._2 + 1, mkey._3), 0)
        val  last2week1hourA = mmmpBd.value.getOrElse((mkey._1 - 2, mkey._2 + 1, mkey._3), 0)
        val  last3week1hourA = mmmpBd.value.getOrElse((mkey._1 - 3, mkey._2 + 1, mkey._3), 0)
        val  last4week1hourA = mmmpBd.value.getOrElse((mkey._1 - 4, mkey._2 + 1, mkey._3), 0)

        val features=List(mkey._2,mkey._3,last1week,last2week,last3week,last4week,last1week1hourB,last2week1hourB,last3week1hourB,last4week1hourB,last1week1hourA,last2week1hourA,last3week1hourA,last4week1hourA)
        val label=mmmpBd.value.getOrElse((mkey._1,mkey._2,mkey._3),0)
        (mkey,features,label)
    }

    //Test week 16
    var LL2=List.empty[(Int,Int,Int)]
      for(j<-6 to 21)
        for(k<-0 to 6){
          LL2=LL2++List((16,j,k))
        }
    val alll2=sc.makeRDD(LL2)              //train range  week9-week15   test week16

    val Test1= alll2.map{
      mkey=>
        val last1week=mmmpBd.value.getOrElse((mkey._1-1,mkey._2,mkey._3),0)
        val last2week=mmmpBd.value.getOrElse((mkey._1-2,mkey._2,mkey._3),0)
        val last3week=mmmpBd.value.getOrElse((mkey._1-3,mkey._2,mkey._3),0)
        val last4week=mmmpBd.value.getOrElse((mkey._1-4,mkey._2,mkey._3),0)

        val  last1week1hourB = mmmpBd.value.getOrElse((mkey._1 - 1, mkey._2 - 1, mkey._3), 0)
        val  last2week1hourB = mmmpBd.value.getOrElse((mkey._1 - 2, mkey._2 - 1, mkey._3), 0)
        val  last3week1hourB = mmmpBd.value.getOrElse((mkey._1 - 3, mkey._2 - 1, mkey._3), 0)
        val  last4week1hourB = mmmpBd.value.getOrElse((mkey._1 - 4, mkey._2 - 1, mkey._3), 0)

        val  last1week1hourA = mmmpBd.value.getOrElse((mkey._1 - 1, mkey._2 + 1, mkey._3), 0)
        val  last2week1hourA = mmmpBd.value.getOrElse((mkey._1 - 2, mkey._2 + 1, mkey._3), 0)
        val  last3week1hourA = mmmpBd.value.getOrElse((mkey._1 - 3, mkey._2 + 1, mkey._3), 0)
        val  last4week1hourA = mmmpBd.value.getOrElse((mkey._1 - 4, mkey._2 + 1, mkey._3), 0)

        val features=List(mkey._2,mkey._3,last1week,last2week,last3week,last4week,last1week1hourB,last2week1hourB,last3week1hourB,last4week1hourB,last1week1hourA,last2week1hourA,last3week1hourA,last4week1hourA)
        val label=mmmpBd.value.getOrElse((mkey._1,mkey._2,mkey._3),0)
        (mkey,features,label)
    }


    ////////////model train/////////////////////
    /**
     *import org.apache.spark.mllib.tree.GradientBoostedTree
     * import org.apache.spark.mllib.tree.configuration.BoostingStrategy
     * import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
     * import org.apache.spark.mllib.util.MLUtils
     */
    import org.apache.spark.mllib.tree.GradientBoostedTrees
    import org.apache.spark.mllib.tree.configuration.BoostingStrategy
    import org.apache.spark.mllib.regression.LabeledPoint
    import org.apache.spark.mllib.linalg.Vectors


    val traindata=Train1.map{
      case (mkey,feature,label)=>
        val vec=Vectors.dense(feature.toArray.map(_.toDouble))
        val lb=LabeledPoint(label.toDouble,vec)
        lb
    }

    val testdata=Test1.map{
      case (mkey,feature,label)=>
        val vec=Vectors.dense(feature.toArray.map(_.toDouble))
        val lb=LabeledPoint(label.toDouble,vec)
        (mkey,feature,label,lb)
    }


    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.numIterations = 3 // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.maxDepth = 5
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(traindata, boostingStrategy)


    val labelsAndPredictions = testdata.map {
      case (mkey,feature,label,lb)=>
      val prediction = model.predict(lb.features)
      (mkey,label, prediction)
    }

    val xxx=labelsAndPredictions.map(x=>math.abs((x._2-x._3.toInt).toDouble/x._2)).filter(x=>x<=0.3)

    def ct(a:Double):Double={
      -10.0/0.3/0.3/0.3*a*a*a+10
    }
    xxx.map(ct(_)).reduce(_+_)


    ///////////////////////////////////////////


















    val aa=sc.makeRDD(1 to 10).map(x=>(x,x))
    val bb=sc.makeRDD(1 to 5).map(x=>(x,x))
    aa.leftOuterJoin(bb)



    stu.saveAsObjectFile(dfspath+"stu")
    //sc.objectFile[mLineData2](dfspath+"stu")




    //=========================================================//














    //stu.groupByKey.map(x=>(x._1,x._2.size)).collect

    //老人卡
    val oldman=dbdatas.filter(x=>x.cardType==1)
    oldman.saveAsObjectFile(dfspath+"oldman")

    sc.objectFile[mLineData2](dfspath+"oldman")

    val common=dbdatas.filter(x=>x.cardType==0)
    common.saveAsObjectFile(dfspath+"common")

    sc.objectFile[mLineData2](dfspath+"common")

    val otherT=dbdatas.filter(x=>x.cardType>2)
    common.saveAsObjectFile(dfspath+"otherT")






    /* val dbdatas = datas.map {
       p =>
         mLineData(p(0).toInt, p(1).toInt, p(2).toInt, p(3).toInt, p(4).toInt, p(5), p(6).toInt, p(7).toInt, p(8).toInt)
     }*/

    val load0 = dbdatas.filter { mline => mline.line == 0 }
    val load1 = dbdatas.filter { mline => mline.line == 1 }

    /**
     * 用户操作
     */
    val userinfo = dbdatas.map { mline => (mline.userID, mline) }.groupByKey
    userinfo.cache()
    //常用用户
    userinfo.filter(x=>x._2.size>5)


    val morethan5 = userinfo.filter { x => x._2.size > 5 }

    val allhour = dbdatas.map { mline => (mline.hour, mline) }.groupByKey.map(x => (x._1, x._2.size / 153))
    ArraytoFile(outputpath+"allhour.txt",allhour.collect)

    val load0hour = load0.map { mline => (mline.hour, mline) }.groupByKey.map(x => (x._1, x._2.size / 153))
    ArraytoFile(outputpath+"load0hour.txt",load0hour.collect)

    val load1hour = load1.map { mline => (mline.hour, mline) }.groupByKey.map(x => (x._1, x._2.size / 153))
    ArraytoFile(outputpath+"load1hour.txt",load1hour.collect)


    //对路线0 进行按星期分割成7个RDD
    val weekofload0: Array[org.apache.spark.rdd.RDD[mLineData2]] = Array.fill(7) {
      null
    }

    //对路线1 进行按星期分割成7个RDD
    val weekofload1: Array[org.apache.spark.rdd.RDD[mLineData2]] = Array.fill(7) {
      null
    }

    //对路线0 7个RDD 进行每小时统计之后的平均值
    val weekofload0avg: Array[org.apache.spark.rdd.RDD[(Int, Int)]] = Array.fill(7) {
      null
    }

    //对路线1 7个RDD 进行每小时统计之后的平均值
    val weekofload1avg: Array[org.apache.spark.rdd.RDD[(Int, Int)]] = Array.fill(7) {
      null
    }

    for (i <- 0 to 6) {
      weekofload0(i) = load0.filter { mline => mline.week == i }
      weekofload1(i) = load1.filter { mline => mline.week == i }
    }


    for (i <- 0 to 6) {
      weekofload0avg(i)=weekofload0(i).map { mline => (mline.hour, mline) }.groupByKey.map(x => (x._1, x._2.size / 153))
      weekofload1avg(i)=weekofload1(i).map { mline => (mline.hour, mline) }.groupByKey.map(x => (x._1, x._2.size / 153))
    }

    for (i <- 0 to 6) {
      ArraytoFile(outputpath+"weekofload0ofweek"+i+".txt",weekofload0avg(i).collect)
      ArraytoFile(outputpath+"weekofload1ofweek"+i+".txt",weekofload1avg(i).collect)
    }


  }














}
*/
