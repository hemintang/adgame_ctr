package com.xm4399.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by hemintang on 17-3-16.
  */
object InfoUtil {

  //用于展示关联前的展示量、点击量、留存度的信息
  def info(df: DataFrame): (Long, Long, Long) = {
    df.rdd.map(row => {
      val (click, remain) = (row.getAs[Int]("isclick").toLong, row.getAs[Int]("isremain").toLong)
      (1l, click, remain)
    }).reduce((t1, t2) => {
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    })
  }

  //用于展示关联后的展示量、点击量、留存度的信息
  def info(rdd: RDD[(String, String, Long, Int, Int, Int, Int)]): (Long, Long, Long) = {
    rdd.map(tuple => {
      val (numShow, click, remain) = (tuple._4.toLong, tuple._5.toLong, tuple._6.toLong)
      (numShow, click, remain)
    }).reduce((t1, t2) =>{
      (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
    })
  }
}
