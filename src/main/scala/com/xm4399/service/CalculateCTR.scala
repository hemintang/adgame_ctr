package com.xm4399.service

import com.xm4399.model.{Game, Query, Session}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by hemintang on 17-3-15.
  */
object CalculateCTR {

//  val INPUTBASEPATH = "hdfs:////hive/warehouse/datamarket/adgame/reality_search_show/datekey="
//  val OUTPUTBASEPATH = "hdfs:///user/hemintang/output/"

  val spark: SparkSession = SparkSession
    .builder()
    .config("spark.hadoop.validateOutputSpecs", "false")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

//    val daysAgo = args(0).toInt
//    val dateKey = DateKeyUtil.getDatekey(daysAgo) //取到输入的日期如，20170303

    val inputPath = "/home/hemintang/input/part-00000"
    val outputPath = "/home/hemintang/output/ctr"
//    val inputPath = INPUTBASEPATH + dateKey
//    val outputPath = OUTPUTBASEPATH + "ctr"
    run(inputPath, outputPath)
  }

  def run(inputPath: String, outputPath: String): Unit ={
    //加载数据，创建概念表t_show_click
    spark.read.orc(inputPath).createOrReplaceTempView("t_show_click")
    //取出需要的6个字段
    val rowDF = spark.sql("select sessionid, query, timestamp, gameid, isclick, isremain from t_show_click")
    //封装成Session对象
    val sessionRDD = toSessionRDD(rowDF)
    //关联查询
    val relevancedSessionRDD = sessionRDD.map(session => session.relevanceQuery)
    //解封装Session
    val relevanceDF = relevancedSessionRDD.flatMap(session => session.unbox).toDF("sessionId", "searchTerm", "timeStamp", "gameId", "show", "click", "remain")
    //打印关联后的展示量、点击量、留存量
//    val rowInfo = rowDF.agg("*" -> "count", "isclick" -> "sum", "isremain" -> "sum").toDF("numShow", "numClick", "numRemain")
//    val relevancedInfo = relevanceDF.agg("show" -> "sum", "click" -> "sum", "remain" -> "sum").toDF("numShow", "numClick", "numRemain")
//    rowInfo.union(relevancedInfo).show()

    relevanceDF.groupBy("searchTerm", "gameId").agg("show" -> "sum", "click" -> "sum", "remain" -> "sum")
      .toDF("searchTerm", "gameId", "numShow", "numClick", "numRemain")
      .sort(-$"numShow")
      .write
      .orc(outputPath)
  }

  //封装成Session对象
  private def toSessionRDD(rowDF: DataFrame): RDD[Session] = {
    //封装成Game对象
    val gameRDD = rowDF.rdd.map(row =>{
      val sessionId = row.getAs[String]("sessionid")
      val searchTerm = row.getAs[String]("query")
      val timeStamp = row.getAs[String]("timestamp").toLong
      val gameId = row.getAs[Int]("gameid")
      val click = row.getAs[Int]("isclick")
      val remain = row.getAs[Int]("isremain")
      ((sessionId, searchTerm, timeStamp), new Game(gameId, click, remain))
    })
    //封装成Query对象
    val queryRDD = gameRDD.groupByKey.map(Query.box)
    //封装成Session对象
    val sessionRDD = queryRDD.groupByKey.map(Session.box)
    sessionRDD
  }
}
