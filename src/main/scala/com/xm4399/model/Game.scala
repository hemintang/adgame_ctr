package com.xm4399.model

/**
  * Created by hemintang on 17-3-15.
  */
class Game(pGameId: Int, pClick: Int, pRemain: Int) extends Serializable{

  val gameId: Int = pGameId
  var numShow: Int = 1
  val click: Boolean = if(0 == pClick) false else true
  val remain: Boolean = if(0 == pRemain) false else true


  def canEqual(other: Any): Boolean = other.isInstanceOf[Game]

  override def equals(other: Any): Boolean = other match {
    case that: Game =>
      (that canEqual this) &&
        gameId == that.gameId
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(gameId)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
