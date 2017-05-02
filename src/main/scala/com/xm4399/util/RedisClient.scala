package com.xm4399.adgame.anticheat.util

import redis.clients.jedis.{JedisPoolConfig, JedisShardInfo, ShardedJedis, ShardedJedisPool}
import redis.clients.util.{Hashing, Sharded}

object RedisClient extends Serializable {

  val redisTimeout = 30000
  val config = new JedisPoolConfig()
  config.setMaxTotal(50)
  config.setMaxIdle(50)
  config.setMaxWaitMillis(30000)
  config.setTestOnBorrow(true)
  val redisAddrs = "10.0.0.94:6395;10.0.0.94:6396;10.0.0.94:6397".split(";")
  //val redisAddrs = "127.0.0.1:6379;127.0.0.1:6379".split(";")

  val redisAddrPorts = Array.ofDim[String](redisAddrs.length, 2)
  for (i <- redisAddrPorts.indices) {
    val sa = redisAddrs(i).split(":")
    redisAddrPorts(i)(0) = sa(0)
    redisAddrPorts(i)(1) = sa(1)
  }

  val jdsInfoList: java.util.List[JedisShardInfo] = new java.util.ArrayList[JedisShardInfo]()
  for (ra <- redisAddrPorts) {
    val jsi = new JedisShardInfo(ra(0), ra(1).toInt)
    jdsInfoList add jsi
  }

  lazy val pool = new ShardedJedisPool(config, jdsInfoList, Hashing.MURMUR_HASH, Sharded.DEFAULT_KEY_TAG_PATTERN)
  lazy val hook = new Thread {
    override def run() = {
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run())

  /**
    * 清空库
    */
  def flushDB = {

    val ports = Array("6395","6396","6397")
    ports.foreach(port => {
      RedisUtils.setMaxWait(-1)
      RedisUtils.getJedis(port.toInt).flushDB()
    })
  }


  def main(args: Array[String]): Unit = {
    val jedis = RedisClient.pool.getResource


    val pipeline = jedis.pipelined()

    pipeline.set("xiaozhang", "redis test")

    jedis.close()
  }

}
