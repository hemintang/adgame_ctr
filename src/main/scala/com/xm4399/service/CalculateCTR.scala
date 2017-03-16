package com.xm4399.service

import com.xm4399.model.{Game, Query, Session}
import com.xm4399.util.InfoUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by hemintang on 17-3-15.
  */
object CalculateCTR {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .config("spark.hadoop.validateOutputSpecs", "false")
      .master("local")
      .getOrCreate()

    val inputPath = "/home/hemintang/input/part-00000"
    val outputPath = "/home/hemintang/output/ctr"

    run(spark, inputPath, outputPath)
  }

  def run(spark: SparkSession, inputPath: String, outputPath: String): Unit ={
    //加载数据，创建概念表t_show_click
    spark.read.orc(inputPath).createOrReplaceTempView("t_show_click")
    //取出需要的6个字段
    val rowDF = spark.sql("select sessionid, query, timestamp, gameid, isclick, isremain from t_show_click")
    //打印关联前的展示量、点击量、留存量
    val (beforeNumShow, beforeNumClick, beforeNumRemain) = InfoUtil.info(rowDF)
    //封装成Session对象
    val sessionRDD = toSessionRDD(rowDF)
    //关联查询
    val relevancedSessionRDD = sessionRDD.map(session => session.relevanceQuery)
    //解封装Session
    val relevanceRDD = relevancedSessionRDD.flatMap(session => session.unbox)
    //打印关联后的展示量、点击量、留存量
    val (afterNumShow, afterNumClick, afterNumRemain) = InfoUtil.info(relevanceRDD)
    println(s"关联前展示量${beforeNumShow}, 点击量${beforeNumClick}, 留存量${beforeNumRemain}")
    println(s"关联后展示量${afterNumShow}, 点击量${afterNumClick}, 留存量${afterNumRemain}")
    relevanceRDD.map(tuple => {
      val (searchTerm, gameId, numShow, numClick, numRemain) = (tuple._2, tuple._4, tuple._5, tuple._6, tuple._7)
      ((searchTerm, gameId), (numShow, numClick, numRemain))
    }).reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2, v1._3 + v2._3))
      .sortBy(tuple => {
        val value = tuple._2
        val numShow = value._1
        numShow
      }, ascending = false).map(tuple => {
      val ((searchTerm, gameId), (numShow, numClick, numRemain)) = tuple
      val line = s"$searchTerm\t$gameId\t$numShow\t$numClick\t$numRemain"
      line
    }).saveAsTextFile(outputPath)
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
