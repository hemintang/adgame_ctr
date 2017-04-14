package com.xm4399.enumeration

/**
  * Created by hemintang on 17-4-14.
  */
object Table extends Enumeration{
  type T = HTable
  val T_adgame_ctr = new HTable("hmt_db", "t_adgame_ctr", "datekey")
  val T_adgame_ctr_midres = new HTable("hmt_db", "t_adgame_ctr_midres", "datekey")
  val Reality_search_show = new HTable("datamarket_adgame", "reality_search_show", "datekey")

}

class HTable(pDatabaseName: String, pTableName: String, pPartitionQualifier: String){

  def databaseName: String = pDatabaseName

  def tableName: String = pTableName

  def partitionQualifier: String = pPartitionQualifier
}
