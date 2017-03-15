package com.xm4399.model

import scala.collection.mutable

/**
  * Created by hemintang on 17-3-15.
  */
class Query(pSearchTerm: String, pTimeStamp: Long, pGames: mutable.Set[Game]) extends Serializable{

  val searchTerm: String = pSearchTerm
  val timeStamp: Long = pTimeStamp
  val games: mutable.Set[Game] = pGames


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
    val games = mutable.Set[Game]()
    gameIter.foreach(game => games += game)
    val query = new Query(searchTerm, timeStamp, games)
    (sessionId, query)
  }
}
