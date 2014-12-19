package com.aquto.slipstream.couchold
import java.net.URI
import com.couchbase.client.CouchbaseConnectionFactory
import scala.collection.JavaConversions._
import com.aquto.slipstream.core.stats.PassthroughStatsCollector
import com.aquto.slipstream.core.stats.StatsCollector
import net.spy.memcached.ConnectionObserver
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
class CouchClientConfigBuilder(servers:List[URI], couchBucket:CouchBucket){
  val bucket = couchBucket
  
  //Additional wrapping of the servers list here is to avoid a nasty UnsupportedOperationException when couch tries
  //to randomize the server list
  private val cf = new CouchbaseConnectionFactory(new java.util.ArrayList(servers), bucket.name, bucket.password)
  private[couchold] var autoShutdownClient = false
  private[couchold] var statsCollector:StatsCollector = PassthroughStatsCollector
  var connLostFuncs:List[(SocketAddress) => Unit] = List()
  var connEstabFuncs:List[(SocketAddress,Int) => Unit] = List()
  
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
  
  /**
   * Adds a function to call when a connection to a server in the cluster is lost.  You can add multiple
   * of these if desired
   * @param func A Function taking a SocketAddress that will be invoked if a connection is lost
   * @return this CouchClientConfigBuilder
   */
  def whenConnectionLost(func:(SocketAddress) => Unit) = {
    connLostFuncs = func :: connLostFuncs
    this
  }
  
  /**
   * Adds a function to call when a connection to a server in the cluster is established.  You can add multiple
   * of these if desired
   * @param func A Function taking a SocketAddress and an Int reconnect count that will be invoked if a connection is established
   * @return this CouchClientConfigBuilder
   */
  def whenConnectionEstablished(func:(SocketAddress,Int) => Unit) = {
    connEstabFuncs = func :: connEstabFuncs
    this
  }  
  
  /**
   * Creates the couch connection factory used to connect to couch
   * @return An instance of CouchbaseConnectionFactory
   */
  private[couchold] def build = cf
}