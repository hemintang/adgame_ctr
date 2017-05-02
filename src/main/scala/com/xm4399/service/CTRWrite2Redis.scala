package com.xm4399.service

import java.text.DecimalFormat

import com.xm4399.util.{DateKeyUtil, RedisClient}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import redis.clients.jedis.Jedis

/**
  * Created by hemintang on 17-4-24.
  * 将计算好的hive表中的t_adgame_ctr缓存到redis中
  */

object CTRWrite2Redis{

  def main(args: Array[String]): Unit = {
    if(1 == args.length){
      val daysAgo = args(0).toInt
      val datekey = DateKeyUtil.getDatekey(daysAgo)

      val spark: SparkSession = SparkSession
        .builder()
        .config("spark.hadoop.validateOutputSpecs", "false")
        .enableHiveSupport()
        .getOrCreate()
      val runner = new CTRWrite2Redi(spark)
      runner.run("20170427")
    }
  }
}

class CTRWrite2Redi(spark: SparkSession) extends Serializable{


  def run(datekey: String): Unit ={
    import spark.implicits._
    //取到当天的汇总
    val numShow_numClick_DF = loadDataFromHive(datekey)
    //计算ctr
    val ctrDF = numShow_numClick_DF.withColumn("ctr", $"numclick" / $"numshow")
    //存进redis中
    ctrDF.foreachPartition(data2Redis(_))
  }

  def data2Redis(iterator: Iterator[Row]): Unit = {

    val jedis = RedisClient.pool.getResource
    val pipeline = jedis.pipelined()
    //保留六位小数
    val decimal = new DecimalFormat(".000000")

    try {
      iterator.foreach(row => {
        val (searchTerm, gameId, ctr) = (row.getAs[String]("searchterm"), row.getAs[Int]("gameid"), row.getAs[Double]("ctr"))
        val ctrFormat = decimal.format(ctr).toFloat
        if(ctr.toFloat != 0){
          pipeline.hset(searchTerm, gameId.toString, ctr.toFloat.toString)
        }
      })
    } catch {
      case e: Exception => println(e.getLocalizedMessage)
    } finally {
      pipeline.sync()
      jedis.close()
    }
  }

  //从hive加载指定分区的数据
  def loadDataFromHive(datekey: String): DataFrame = {
    val ctrDF = spark.sql(
      s"""
         |select searchterm, gameid, numshow, numclick from default.t_adgame_ctr
         |where datekey = 'sum_$datekey'
       """.stripMargin)
    ctrDF
  }

}
