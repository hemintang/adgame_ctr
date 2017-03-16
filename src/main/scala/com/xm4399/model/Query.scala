package com.xm4399.model

import scala.collection.mutable

/**
  * Created by hemintang on 17-3-15.
  */
class Query(pSearchTerm: String, pTimeStamp: Long) extends Serializable{

  val searchTerm: String = pSearchTerm
  val timeStamp: Long = pTimeStamp
  var games: mutable.Set[Game] = _
  var click: Boolean = false

  def isClick: Boolean = click

  //关联查询
  def relevance(otherQuery: Query): Unit ={
    for(game <- otherQuery.games){
      //如果没有这个游戏则关联(添加)
      if(!this.games.contains(game)){
        this.games += game
      }else if(game.isClick){ //包含，但是被点击了，展示量+1
        this.games remove game
        game.numShow = 2
        this.games += game
      }
    }
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[Query]

  override def equals(other: Any): Boolean = other match {
    case that: Query =>
      (that canEqual this) &&
        searchTerm == that.searchTerm &&
        timeStamp == that.timeStamp
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(searchTerm, timeStamp)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}

object Query{
  //封装成Query对象
  def box(tuple: ((String, String, Long), Iterable[Game])): (String, Query) = {
    val (sessionId, searchTerm, timeStamp) = tuple._1
    val gameIter = tuple._2
    val query = new Query(searchTerm, timeStamp)
    val games = mutable.Set[Game]()
    for(game <- gameIter){
      games += game
      if(game.isClick) query.click = true
    }
    (sessionId, query)
  }
}
