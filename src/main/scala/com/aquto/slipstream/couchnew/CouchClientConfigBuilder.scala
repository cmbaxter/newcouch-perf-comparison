package com.aquto.slipstream.couchnew
import java.net.URI
import scala.collection.JavaConversions._
import com.aquto.slipstream.core.stats.PassthroughStatsCollector
import com.aquto.slipstream.core.stats.StatsCollector
import java.net.SocketAddress

/**
 * Represents a bucket to connect to
 * @param name The name of the bucket to connect to
 * @param password The password for that bucket
 */
case class CouchBucket(name:String, password:String)

/**
 * Class used to create the configuration for connecting to couchbase
 * @param servers A list of uris representing the servers to connect to
 */
class CouchClientConfigBuilder(val servers:List[String], val couchBucket:CouchBucket){
  
  private[couchnew] var autoShutdownClient = false
  private[couchnew] var statsCollector:StatsCollector = PassthroughStatsCollector
  
  /**
   * Set this client to auto register a shutdown hook on JVM exit
   * @return this CouchClientConfigBuilder
   */
  def withAutoShutdown:CouchClientConfigBuilder = {
    autoShutdownClient = true
    this
  }
  
  /**
   * Sets up the couch client with a specific stats collector to use
   * @param stats The StatsCollector to use for collecting various couch stats
   * @return this CouchClientConfigBuilder
   */
  def withStatsCollector(stats:StatsCollector) = {
    statsCollector = stats
    this
  }
}