package com.xm4399.service

import com.xm4399.enumeration.{Table, HTable}
import com.xm4399.model.{Game, Query, Session}
import com.xm4399.util.DateKeyUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
/**
  * Created by hemintang on 17-3-15.
  * 增量式子CTR计算
  */
object CalculateCTR extends Serializable{

  //展示量的阙值
  val NUMSHOWTHRESHOLD: Double = 10000d
  //计算的是哪天的数据
  var dateKey: String = _
  //数据库
  var databaseName: String = _
  val spark: SparkSession = SparkSession
    .builder()
    .config("spark.hadoop.validateOutputSpecs", "false")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    if(2 == args.length){
      val daysAgo = args(0).toInt
      CalculateCTR.databaseName = args(1)
      CalculateCTR.dateKey = DateKeyUtil.getDatekey(daysAgo) //取到输入的日期如，20170303
      run(daysAgo)
    }

  }

  def run(daysAgo: Int): Unit ={

    //计算某一天的ctr, true表示保存中间计算结果
    val oneDayCTR = CalculateCTR.oneDayCTR(true)
    //防止容错机制多次重算，所以直接从hive表里取
    //    val oneDayCTR = loadDFFromHive(Table.T_adgame_ctr, dateKey, "searchterm, gameid, numshow, numclick, numremain")
    //取到前一天的汇总ctr
    val sumDateKey = DateKeyUtil.getDatekey(daysAgo + 1)
    //加载前一天汇总的ctr
    val sumCTRDF: DataFrame = loadDFFromHive(Table.T_adgame_ctr, s"sum_$sumDateKey", "searchterm, gameid, numshow, numclick, numremain")
    //将某天的ctr和前一天的汇总合并
    val newSumCTRDF = mergeCTR(sumCTRDF, oneDayCTR)
    //将新汇总的ctr存进hive表
    saveDF(newSumCTRDF, Table.T_adgame_ctr, s"sum_$dateKey")
  }

  //将DataFrame存进hive表中
  def saveDF(df: DataFrame, tableDesc: HTable, partitionName: String): Unit ={
    df.createOrReplaceTempView("t_df")
    spark.sql(s"use ${CalculateCTR.databaseName}")
    spark.sql(
      s"""
         |insert overwrite table ${tableDesc.tableName}
         |partition (${tableDesc.partitionQualifier} = '$partitionName')
         |select * from t_df
       """.stripMargin)
  }

  //汇总ctr
  def mergeCTR(beformCTRDF: DataFrame, oneCTRDF: DataFrame): DataFrame = {
    val unionedDF = beformCTRDF.union(oneCTRDF).groupBy("searchterm", "gameid").agg("numshow" -> "sum", "numclick" -> "sum", "numremain" -> "sum")
      .toDF("searchTerm", "gameId", "numShow", "numClick", "numRemain")
    //策略：展示量不超过10000
    val overDF = unionedDF.where(s"numShow > $NUMSHOWTHRESHOLD")
    val unOverDF = unionedDF.where(s"numshow <= $NUMSHOWTHRESHOLD")
    val detailOverDF = overDF.withColumn("newNumShow", lit(NUMSHOWTHRESHOLD))
      .withColumn("newNumClick", $"numClick" / $"numShow" * NUMSHOWTHRESHOLD)
      .withColumn("newNumRemain", $"numRemain" / $"numShow")
      .select("searchTerm", "gameId", "newNumShow", "newNumClick", "newNumRemain").toDF("searchTerm", "gameId", "numShow", "numClick", "numRemain")

    detailOverDF.union(unOverDF)
  }
  //从hive表中加载数据
  def loadDFFromHive(tableDesc: HTable, partitionName: String, columns: String, databaseName: String = "default"): DataFrame = {
    spark.sql(
      s"""
         |select $columns from $databaseName.${tableDesc.tableName}
         |where datekey = '$partitionName'
       """.stripMargin)
  }

  //计算一天的数据
  def oneDayCTR(saveMidRes: Boolean): DataFrame ={
    //加载数据，取出需要的7个字段
    val rowDF = loadDFFromHive(Table.Reality_search_show, dateKey, "udid, sessionid, query, timestamp, gameid, isclick, isremain", "datamarket_adgame")
    //封装成Session对象
    val sessionRDD = toSessionRDD(rowDF)
    //关联查询
    val relevancedSessionRDD = sessionRDD.map(session => session.relevanceQuery)
    //解封装Session
    val ctrMidDF = relevancedSessionRDD.flatMap(session => session.unbox).toDF("udId", "sessionId", "searchTerm", "queryTimeStamp", "gameId", "numShow", "numClick", "numRemain")
    //保存中间结果
    if(saveMidRes){
      saveDF(ctrMidDF, Table.T_adgame_ctr_midres, dateKey)
    }
    //聚合展示量的、点击量、留存量
    val ctrDF = ctrMidDF.groupBy("searchTerm", "gameId").agg("numShow" -> "sum", "numClick" -> "sum", "numRemain" -> "sum")
      .toDF("searchTerm", "gameId", "numShow", "numClick", "numRemain")
    saveDF(ctrDF, Table.T_adgame_ctr, dateKey)
    ctrDF
  }

  //封装成Session对象
  private def toSessionRDD(rowDF: DataFrame): RDD[Session] = {
    //封装成Game对象
    val gameRDD = rowDF.rdd.map(row =>{
      val udId = row.getAs[String]("udid")
      val sessionId = row.getAs[String]("sessionid")
      val searchTerm = row.getAs[String]("query")
      val timeStamp = row.getAs[String]("timestamp").toLong
      val gameId = row.getAs[Int]("gameid")
      val click = row.getAs[Int]("isclick")
      val remain = row.getAs[Int]("isremain")
      ((udId, sessionId, searchTerm, timeStamp), new Game(gameId, click, remain))
    })
    //封装成Query对象
    val queryRDD = gameRDD.groupByKey.map(Query.box)
    //封装成Session对象
    val sessionRDD = queryRDD.groupByKey.map(Session.box)
    sessionRDD
  }
}
