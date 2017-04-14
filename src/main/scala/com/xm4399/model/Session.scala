package com.xm4399.model

import scala.collection.mutable.ListBuffer

/**
  * Created by hemintang on 17-3-15.
  */
class Session(pUdId: String, pSessionId: String, pQuerys: List[Query]) extends Serializable{

  val udId: String = pUdId
  val sessionId: String = pSessionId
  val querys: List[Query] = pQuerys

  //session中的query进行关联
  def relevanceQuery: Session = {
    for(index <- 1 until querys.size){
      val query = querys(index)
      //如果没有被点击，则和下一个时间戳的query关联
      if(!query.isClick){
        val lastQuery = querys(index - 1)
        query.relevance(lastQuery)
      }
    }
    this
  }

  //解封装成(sessionId, searchTerm, timeStamp, gameId, numShow, numClick, numRemain)
  def unbox: List[(String, String, String, Long, Int, Int, Int, Int)] = {
    val records = new ListBuffer[(String, String, String, Long, Int, Int, Int, Int)]
    for(query <- querys){
      val searchTerm = query.searchTerm
      val timeStamp = query.timeStamp
      for(game <- query.games){
        val gameId = game.gameId
        val numShow = game.numShow
        val numClick = if(game.click) 1 else 0
        val numRemain = if(game.remain) 1 else 0
        val record = (udId, sessionId, searchTerm, timeStamp, gameId, numShow, numClick, numRemain)
        records += record
      }
    }
    records.toList
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Session]

  override def equals(other: Any): Boolean = other match {
    case that: Session =>
      (that canEqual this) &&
        sessionId == that.sessionId
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(sessionId)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object Session{
  //封装成Session对象
  def box(tuple: ((String, String), Iterable[Query])): Session = {
    val (udId, sessionId) = tuple._1
    val queryIter = tuple._2
    //按时间逆序
    val querys = queryIter.toList.sortBy(query => -query.timeStamp)
    new Session(udId, sessionId, querys)
  }
}
