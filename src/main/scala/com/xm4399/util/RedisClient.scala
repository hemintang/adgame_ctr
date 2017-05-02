package com.xm4399.util

import redis.clients.jedis.{JedisPoolConfig, JedisShardInfo, ShardedJedisPool}
import redis.clients.util.{Hashing, Sharded}

object RedisClient extends Serializable {

  val redisTimeout = 30000
  val config = new JedisPoolConfig()
  config.setMaxTotal(50)
  config.setMaxIdle(50)
  config.setMaxWaitMillis(30000)
  config.setTestOnBorrow(true)
  val redisAddrs = "10.0.0.92:6380".split(";")
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


}
