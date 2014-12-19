package com.aquto.slipstream.couchnew
import java.net.URI
import java.net.InetSocketAddress
import scala.collection.JavaConversions._
import java.util.concurrent.TimeoutException
import scala.collection.mutable.MapBuilder
import scala.concurrent._
import duration._
import scala.util.{Try, Success, Failure}
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.TimeUnit
import com.aquto.slipstream.core._
import scala.sys.ShutdownHookThread
import com.aquto.slipstream.core.stats.StatsCollector
import com.aquto.slipstream.core.stats.PassthroughStatsCollector
import com.aquto.slipstream.core.stats.RegisteredStat
import java.net.SocketAddress
import language.existentials
import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment
import rx.lang.scala.JavaConversions._
import rx.lang.scala.Observable
import com.couchbase.client.java.bucket.AsyncBucketManager
import com.couchbase.client.java.CouchbaseAsyncCluster
import com.couchbase.client.java.AsyncBucket
import akka.event.LoggingAdapter
import akka.event.EventBus
import akka.event.Logging
import akka.event.LoggingBus

object CouchClient{
  val StatsHeading = "com.aquto.slipstream.couch"
  val BucketStatProto = RegisteredStat("", "", StatsHeading)
  val TimeoutsStat = "timeouts"
  val ExceptionsStat = "exceptions"
  val FailureRate = "failure-rate"
    
  /**
   * Pimped out Try with some additional functionality
   */
  implicit class PimpedTry[A](tr:Try[A]){
    
    /**
     * Replicates the fold functionality from scalaz Validation.  Allows you to convert a Try for type A
     * to an instance of a new type B, handling Success and Failure separately
     * @param ex A function to invoke to get the type B when the Try is a Failure
     * @param s A function to invoke when the Try is a Success
     * @return an instance of type B
     */
    def fold[B](ex:(Throwable) => B, s:(A) => B) = tr match{
      case Success(success) => s(success)
      case Failure(excep) => ex(excep)
    }
  } 
  
  /**
   * Exception class to represent the situation where we were not able to properly initialize and connect to the bucket for a CouchClient
   */
  class NotConnectedException(message:String) extends Exception(message)
  
  /**
   * Creates a new instance of CouchClient
   * @param config The config builder for the client to create
   * @param logBus The LoggingBus to hook logging into
   */
  def apply(config:CouchClientConfigBuilder, logBus:LoggingBus):CouchClient = new CouchClient(config, logBus)
  
  //The single AQBinaryTranscoder to use for initializing any buckets that are connected to
  val BinaryTranscoder = new AQBinaryTranscoder
  
  //Used so that we can use makePromise on flush too
  private[couchnew] case object FlushOp extends Operation[Boolean]{
    def name = "flush"
  }
}

/**
 * Holds the single instances of CouchbaseAsyncCluster keyed by the server list that makes up the cluster
 */
private[couchnew] object CouchClusters{
  val Env = DefaultCouchbaseEnvironment.create
  val TheInstance = new CouchClusters
}

private[couchnew] class CouchClusters{  
  var clusters:Map[List[String], CouchbaseCluster] = Map.empty
  
  /**
   * Gets a cluster for the supplied server list, creating it if it has yet to be created
   * @param servers The server list for the cluster to get
   * @return CouchbaseAsyncCluster
   */
  def clusterFor(servers:List[String]) = {
    val key = servers.sorted
    clusters.getOrElse(key, {
      val cluster = newCluster(servers)
      clusters += ((key, cluster))
      cluster
    })
  } 
  
  /**
   * Creates a new instance of CouchbaseAsyncCluster if we don't already have one for the servers list supplied
   * @param servers The servers list to create the cluster for
   * @return CouchbaseCluster
   */
  def newCluster(servers:List[String]):CouchbaseCluster = CouchbaseCluster.create(CouchClusters.Env, servers)
}


/**
 * Client to a couchbase server.  This class exposes functions to interact with the data stored
 * in couchbase or memcached bucket types in a couchbase server.
 * @param config An instance of CouchClientConfigBuilder that defines how to connect to couch
 * @param logBus A LoggingBus so this client can hook into the logging system
 * @param ex An implicit ExecutionContext used for execution of the Promises/Futures
 */
class CouchClient(config:CouchClientConfigBuilder, logBus:LoggingBus){
  import CouchClient._
  
  private[couchnew] val log = initLog
  private[couchnew] var bucketOpt:Option[AsyncBucket] = None
  private val cluster = initCluster
  
  //Just going to use connection logic that forces the cluster to be running for now.  The dalayed init logic with a CouchbaseAsyncCluster
  //does some interesting things when multiple buckets are being opened at the same time and we need to avoid that
  //until it can be fixed.  
  bucketOpt = Option(cluster.openBucket(config.couchBucket.name, config.couchBucket.password, List(BinaryTranscoder)).async)
  
  //Add in the handling to set the bucket once a connection has been established
  /*bucketObs.subscribe(
    bucket => bucketOpt = Some(bucket),
    ex => log.error(ex, "Unable to establish a connection to bucket {}", config.couchBucket.name)
  )*/
  
  
  val stats = config.statsCollector
  if (config.autoShutdownClient) ShutdownHookThread(shutdown())
  
  //Register operation timing bucket stats
  private[couchnew] val stat = BucketStatProto.copy(groupName = config.couchBucket.name)
  Operations.AllOperations foreach{ op =>    
    stats.registerTimer(stat.copy(key = op))
  }
  
  //Register exception based meter stats  
  stats.registerMeter(stat.copy(key = TimeoutsStat))
  stats.registerMeter(stat.copy(key = ExceptionsStat))
  
  //Register total exception (regular exceps and timeouts) rate stat
  val allCalls = stat.copy(key = Operations.AllOp)
  stats.registerRatioMeters(List(stat.copy(key = TimeoutsStat), stat.copy(key = ExceptionsStat)), List(allCalls), FailureRate)
  
  /**
   * Initializes the CouchbaseAsyncCluster that this client uses by getting it from the CouchClusters singleton
   * @return CouchbaseAsyncCluster
   */
  private[couchnew] def initCluster = CouchClusters.TheInstance.clusterFor(config.servers)
  
  /**
   * Initializes the LoggingAdapter that this client will use
   * @return LoggingAdapter
   */
  private[couchnew] def initLog = Logging(logBus, "CouchClient")
  
  /**
   * Makes a promise that will be completed then the supplied Observable completes with an item, also
   * adding in timeout handling
   * @param op The operation this promise is for
   * @param obs The observable to bridge into the promise
   * @param timeout The timeout to use for failing the future if the observable does not complete in time
   * @return a Future for type T
   */
  private def makePromise[T](op:Operation[_], obs:Observable[T])(implicit timeout:Duration):Future[T] = {
    val prom = Promise[T]()
    val startTime = System.currentTimeMillis()
    
    def handleFail(ex:Throwable) {
      log.error(ex, "Error performing a couch operation")
      prom.failure(ex)
      
      val statKey = ex match{
        case te:TimeoutException => TimeoutsStat 
        case other => ExceptionsStat 
      }
      stats.markMeter(stat.copy(key = statKey), 1)
      updateTiming(startTime, op)
    }
    
    def handleSuccess(result:T) {
      prom.success(result)
      updateTiming(startTime, op)
    }
   
    obs.
      timeout(timeout).
      subscribe(handleSuccess,handleFail)    
    prom.future
  }
  
  /**
   * Updates the timing stats for the supplied op
   * @param startTime The time the completed op started execution
   * @param op The op to update the stats for
   */
  def updateTiming(startTime:Long, op:Operation[_]){
    val time = System.currentTimeMillis() - startTime            
    stats.updateTime(stat.copy(key = op.name), time)		
    stats.updateTime(stat.copy(key = Operations.AllOp), time)     
  }
  
  /**
   * Invokes the function f passing in the AsyncBucket wrapped by bucketOpt if it's a Some.  If not, a failed
   * future is returned instead
   * @param f A function taking an AsyncBucket and returning a Future for type T
   * @return a Future for type T
   */
  def withBucket[T](f:AsyncBucket => Future[T]):Future[T] = {
    bucketOpt.fold[Future[T]](Future.failed(new NotConnectedException(s"Bucket ${config.couchBucket.name} is not initialized yet")))(f)
  }
  
  /**
   * Executes a read based operation against couch, returning a Future to obtain the result in an async manner
   * @param op A ReadOperation that will be submitted to the couch server to fetch a record
   * @param timeout An implicit timeout to use for the async callbacks for the future returned
   * @return A Future representing an optional response of type RT
   */
  def <--[RT](op:ReadOperation[RT])(implicit timeout:Duration):Future[Option[RT]] = withBucket{ bucket =>
    val obs = op.execute(bucket).map(Some(_)).elementAtOrDefault(0, None)
    makePromise(op, obs)          
  }
  
  /**
   * Executes a storage based operation (add, delete, etc...) against couch, returning a Future for
   * obtaining the result of that storage operation in an async manner
   * @param op A StorageOperation to be executed against couch
   * @param timeout An implicit timeout to use for the async callbacks for the future returned
   * @return A Future for obtaining the result of the storage operation
   */
  def -->[T, RT](op:StoreOperation[T, RT])(implicit timeout:Duration):Future[RT] = withBucket{ bucket =>
    val obs = op.execute(bucket)
    makePromise(op, obs) 
  }

  /**
   * Flushes a bucket, using the flush method of the underlying CouchbaseClient
   * @param timeout A timeout value to use in waiting for a response from the flush
   * @return a Future wrapping a Boolean indicator for the success of the flush
   */
  def flush()(implicit timeout:Duration):Future[Boolean] = withBucket{ bucket =>
    val obs:Observable[AsyncBucketManager] = bucketManager(bucket)
    makePromise(FlushOp, obs.flatMap(_.flush()).map(_.booleanValue()))
  }
  
  /**
   * Gets an observable for the AsyncBucketManager for the supplied bucket
   * @param bucket The bucket to get the manager for
   * @return an Observable for an AsyncBucketManager 
   */
  private[couchnew] def bucketManager(bucket:AsyncBucket) = bucket.bucketManager()
  
  /**
   * Shuts down the client.
   * @return a Boolean representing the result of the shutdown request
   */
  def shutdown():Boolean = bucketOpt.map(_.close().toBlocking().first().booleanValue()).getOrElse(false)
}