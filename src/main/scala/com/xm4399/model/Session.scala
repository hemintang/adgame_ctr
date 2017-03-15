package com.xm4399.model

/**
  * Created by hemintang on 17-3-15.
  */
class Session(pSessionId: String, pQuerys: List[Query]) extends Serializable{

  val sessionId: String = pSessionId
  val querys: List[Query] = pQuerys


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
  def box(tuple: (String, Iterable[Query])): Session = {
    val (sessionId, queryIter) = tuple
    //按时间逆序
    val querys = queryIter.toList.sortBy(query => -query.timeStamp)
    new Session(sessionId, querys)
  }
}
