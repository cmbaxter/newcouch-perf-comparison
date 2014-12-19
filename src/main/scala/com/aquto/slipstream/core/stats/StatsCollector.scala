package com.aquto.slipstream.core.stats


/**
 * Represents a stat that will be registered and then collected, with props for the key of the stat
 * as well as the high level grouping for the stat
 */
case class RegisteredStat(key:String, groupName:String, heading:String, scope:Option[String] = None)

object StatsCollector{
  val SqlHeading = "com.aquto.slipstream.sql"
  val CacheHeading = "com.aquto.slipstream.cache"  
  val MailboxThroughput = RegisteredStat("mailbox-throughput", "", SqlHeading)
  val MailboxSize = RegisteredStat("", "", SqlHeading, Some("MailboxSize"))
  val SqlTiming = RegisteredStat("", "", SqlHeading, Some("SqlTiming"))
  val SqlFailures = RegisteredStat("failures", "", SqlHeading)
  val SqlReconnects = RegisteredStat("reconnects", "", SqlHeading)
  val CachedEntityStats = RegisteredStat("", "", CacheHeading)
  val AllEntitiesStat = "All"
  val AllSqlOpsStat = "all"
  val CacheHits = "hits"
  val CacheMisses = "misses"
  val CacheHitRatio = "hit-ratio"
  val CacheGets = "gets"
  val CacheSets = "sets"
  val CacheDeletes = "deletes"
}

/**
 * Defines behaviors related to collecting various stats within slipstream related services
 */
trait StatsCollector {
  
  def registerTimer(stat:RegisteredStat):StatsCollector
  
  def registerCounter(stat:RegisteredStat):StatsCollector
  
  def registerMeter(stat:RegisteredStat):StatsCollector
  
  def registerRatioMeters(numerator:RegisteredStat, denominator:RegisteredStat, name:String):StatsCollector
  
  def registerRatioMeters(numerators:List[RegisteredStat], denominators:List[RegisteredStat], name:String ):StatsCollector

  /**
   * Times a specific function, reporting the timings against the supplied key
   */
  def time[T](stat:RegisteredStat, func: => T):T
  
  /**
   * Times a specific function, reporting the timings against the supplied key
   */
  def time[T](stats:List[RegisteredStat], func: => T):T
  
  def updateTime(stat:RegisteredStat, time:Long):Unit
  
  def incr(stat:RegisteredStat):Unit
  
  def decr(stat:RegisteredStat):Unit
  
  def markMeter(stat:RegisteredStat, count:Int = 1):Unit
}

/**
 * Default impl of StatsCollector that does not do any actual stats collection
 */
object PassthroughStatsCollector extends StatsCollector{
  
  def registerTimer(stat:RegisteredStat) = this
  
  def registerCounter(stat:RegisteredStat) = this
  
  def registerMeter(stat:RegisteredStat) = this
  
  def registerRatioMeters(numerator:RegisteredStat, denominator:RegisteredStat, name:String ) = this
  def registerRatioMeters(numerators:List[RegisteredStat], denominators:List[RegisteredStat], name:String ) = this
  
  def time[T](stat:RegisteredStat, func: => T):T = func
  
  def time[T](stats:List[RegisteredStat], func: => T):T = func
  
  def updateTime(stat:RegisteredStat, time:Long){}
  
  def incr(stat:RegisteredStat) {}
  
  def decr(stat:RegisteredStat) {}
  
 def markMeter(stat:RegisteredStat, count:Int = 1) {}
}