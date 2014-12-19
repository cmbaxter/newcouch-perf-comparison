package com.aquto.slipstream.core.stats

import com.yammer.metrics._
import com.yammer.metrics.core._
import java.util.concurrent.TimeUnit
import java.lang.management.ManagementFactory
import javax.management.ObjectName

/**
 * An implementation of StatsCollector that uses the Yammer Metrics library to perform JMX instrumented stats collection
 */
class MetricsStatsCollector extends StatsCollector{
  val registry = Metrics.defaultRegistry() 
  val unknownTiming = registry.newTimer(new MetricName("com.aquto.slipstream", "Unknown", "unknown-timings"), TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
  val unknownCounter = registry.newCounter(new MetricName("com.aquto.slipstream", "Unknown", "unknown-counter"))
  val unknownMeter = registry.newMeter(new MetricName("com.aquto.slipstream", "Unknown", "unknown-meter"), "occurrences", TimeUnit.SECONDS)
  private[this] var timers = Map[RegisteredStat,Timer]()
  private[this] var counters = Map[RegisteredStat, Counter]()
  private[this] var meters = Map[RegisteredStat, Meter]()
  private[this] val mbeanServer = ManagementFactory.getPlatformMBeanServer()
  
  def registerTimer(stat:RegisteredStat) = {
    val timer = registry.newTimer(nameFor(stat), TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
    timers = timers ++ Map(stat -> timer)
    this
  }
  
  private[this] def nameFor(stat:RegisteredStat) = stat.scope match{
    case Some(scope) => new MetricName(stat.heading, stat.groupName, stat.key, scope)
    case None => new MetricName(stat.heading, stat.groupName, stat.key)
  }
  
  def registerCounter(stat:RegisteredStat) = {
    val counter = registry.newCounter(nameFor(stat))
    counters = counters ++ Map(stat -> counter)    
    this
  }
  
  def registerMeter(stat:RegisteredStat) = {
    val meter = registry.newMeter(nameFor(stat), "occurrences", TimeUnit.SECONDS)
    meters = meters ++ Map(stat -> meter)
    this
  }
  
  def registerRatioMeters(numerator:RegisteredStat, denominator:RegisteredStat, name:String) = registerRatioMeters(List(numerator), List(denominator), name)
  
  def registerRatioMeters(numerators:List[RegisteredStat], denominators:List[RegisteredStat], name:String) = {
    def findMeter(stat:RegisteredStat):Metered = (meters.get(stat), timers.get(stat)) match{
      case (Some(meter), _) => meter
      case (None, Some(meter)) => meter
      case _ => 
        val meter = registry.newMeter(nameFor(stat), "occurrences", TimeUnit.SECONDS)
        meters = meters ++ Map(stat -> meter)
        meter
    }
    
    val numMeters = numerators map findMeter
    val denomMeters = denominators map findMeter
    val objName = new ObjectName(nameFor(numerators.head.copy(key = name)).getMBeanName())
    val gauge = new RateGauge(numMeters, denomMeters)
    mbeanServer.registerMBean(gauge, objName)
    this
  }   
  
  def time[T](stat:RegisteredStat, func: => T):T = time(List(stat), func)
  
  def time[T](stats:List[RegisteredStat], func: => T):T = {
    val start = System.currentTimeMillis
    val result = func
    val timed = System.currentTimeMillis - start
    stats foreach (updateTime(_, timed))
    result
  }  
  
  def updateTime(stat:RegisteredStat, time:Long){
    val timer = timers.getOrElse(stat, unknownTiming)
    timer.update(time, TimeUnit.MILLISECONDS)    
  } 
  
  def incr(stat:RegisteredStat) = count(stat, _.inc())
  
  def decr(stat:RegisteredStat) = count(stat, _.dec())
  
  private[this] def count(stat:RegisteredStat, func:(Counter) => Unit) {
    val counter = counters.getOrElse(stat, unknownCounter)
    func(counter)
  }
  
  def markMeter(stat:RegisteredStat, count:Int = 1){
    val meter = meters.getOrElse(stat, unknownMeter)
    meter.mark(count)
  } 
}

/**
* MBean interface for a rate gauge that shows percents for total, one, five and fifteen minute rates
*/ 
trait RateGaugeMBean{
    
  /**
   * Gets the overall rate (non decaying) of occurrences
   * @return A double representing the overall rate
   */
  def getOverallRate:Double
    
  /**
   * Gets the one minute rate of occurrences
   * @return A double representing the one minute rate
   */    
  def getOneMinuteRate:Double
    
  /**
   * Gets the five minute rate of occurrences
   * @return A double representing the five minute rate
   */    
  def getFiveMinuteRate:Double
    
  /**
   * Gets the fifteen minute rate of occurrences
   * @return A double representing the fifteen minute rate
   */    
  def getFifteenMinuteRate:Double
}
  
/**
 * Impl class for the RateGaugeMBean
 */
class RateGauge(numerators:List[Metered], denominators:List[Metered]) extends RateGaugeMBean{
  def fold(meters:List[Metered], f: Metered => Double):Double = meters.foldLeft(0.0)(_+f(_))
  def percent(f: Metered => Double):Double = {
    val denom = fold(denominators, f)
    if (denom == 0.0) denom
    else{
      (fold(numerators, f)/denom)*100  
    }      
  }
  def getOverallRate = percent(_.count)
  def getOneMinuteRate = percent(_.oneMinuteRate)
  def getFiveMinuteRate = percent(_.fiveMinuteRate)
  def getFifteenMinuteRate = percent(_.fifteenMinuteRate) 
} 