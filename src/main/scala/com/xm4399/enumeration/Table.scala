package com.xm4399.enumeration

/**
  * Created by hemintang on 17-4-14.
  */
object Table extends Enumeration{
  type T = HTable
  val T_adgame_ctr = new HTable("t_adgame_ctr", "datekey")
  val T_adgame_ctr_midres = new HTable("t_adgame_ctr_midres", "datekey")
  val Reality_search_show = new HTable("reality_search_show", "datekey")

}

class HTable(pTableName: String, pPartitionQualifier: String){

  def tableName: String = pTableName

  def partitionQualifier: String = pPartitionQualifier
}
