package service

import com.xm4399.model.{Game, Query, Session}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by hemintang on 17-3-15.
  */
object CalculateCTR {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
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
    //封装成Session对象
    val sessionRDD = toSessionRDD(rowDF)
    sessionRDD.take(3).foreach(session => println(session.sessionId))
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
