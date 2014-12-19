package com.aquto.slipstream.couchold
import net.spy.memcached._
import java.net.URI
import java.net.InetSocketAddress
import scala.collection.JavaConversions._
import com.couchbase.client.CouchbaseConnectionFactory
import com.couchbase.client.CouchbaseClient
import net.spy.memcached.CachedData
import net.spy.memcached.transcoders.Transcoder
import net.spy.memcached.ops.{GetOperation, OperationStatus, GetsOperation, GetAndTouchOperation, OperationCallback, Mutator, CASOperationStatus, CancelledOperationStatus, TimedOutOperationStatus, KeyedOperation, ConcatenationType}
import net.spy.memcached.ops.{StoreType => CouchStoreType, StoreOperation => CouchStoreOp}
import java.util.concurrent.TimeoutException
import net.spy.memcached.util.StringUtils
import scala.collection.mutable.MapBuilder
import net.spy.memcached.protocol.binary.BinaryOperationFactory
import net.spy.memcached.ops.DeleteOperation
import scala.concurrent._
import duration._
import scala.util.{Try, Success, Failure}
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import org.jboss.netty.util.HashedWheelTimer
import org.jboss.netty.util.TimerTask
import java.util.concurrent.TimeUnit
import org.jboss.netty.util.Timeout
import com.aquto.slipstream.core._
import net.spy.memcached.compat.log.Logger
import scala.sys.ShutdownHookThread
import com.aquto.slipstream.core.stats.StatsCollector
import com.aquto.slipstream.core.stats.PassthroughStatsCollector
import com.aquto.slipstream.core.stats.RegisteredStat
import java.net.SocketAddress
import language.existentials

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
  
  //Extractor object for checking for empty keys
  object EmptyKeyedOp{
    def unapply(op:KeyBasedOperation[_, _]) = {
      if (Option(op.key).map(_.toString.trim).getOrElse("").isEmpty) Some(op)
      else None
    }
  }
  
  def apply(config:CouchClientConfigBuilder)(implicit ex:ExecutionContext):CouchClient = new CouchClientImpl(config)
}

trait CouchClient{
  
  /**
   * Executes a read based operation against couch, returning a Future to obtain the result in an async manner
   * @param op A ReadOperation that will be submitted to the couch server to fetch a record
   * @param timeout An implicit timeout to use for the async callbacks for the future returned
   * @return A Future representing an optional response of type RT
   */  
  def <--[RT](op:ReadOperation[_, RT])(implicit timeout:Duration):Future[Option[RT]]
  
  /**
   * Executes a storage based operation (add, delete, etc...) against couch, returning a Future for
   * obtaining the result of that storage operation in an async manner
   * @param op A StorageOperation to be executed against couch
   * @param timeout An implicit timeout to use for the async callbacks for the future returned
   * @return A Future for obtaining the result of the storage operation
   */
  def -->[T, RT](op:StoreOperation[T, RT])(implicit timeout:Duration):Future[RT]  
  
  /**
   * Flushes a bucket, using the flush method of the underlying CouchbaseClient
   * @param delay An optional delay time to pass to the flush call, defaulting to no delay
   * @return a Future wrapping a Boolean indicator for the success of the flush
   */
  def flush(delay:Duration = -1 seconds):Future[Boolean]  
  
  /**
   * Shuts down the client.
   * @param wait An optional amount of time to wait for the queues to die down before moving forward with the shutdown
   * @return a Boolean representing the result of the shutdown request
   */
  def shutdown(wait:Option[Duration] = None):Boolean
  
  /**
   * Returns if this client was properly initialized (able to initially connect to the cluster)
   * @return a Boolean indicating if we were able to propely connect to the cluster on creation
   */
  def initialized:Boolean
}

/**
 * Client to a couchbase server.  This class exposes functions to interact with the data stored
 * in couchbase or memcached bucket types in a couchbase server.
 * @param config An instance of CouchClientConfigBuilder that defines how to connect to couch
 * @param ex An implicit ExecutionContext used for execution of the Promises/Futures
 */
class CouchClientImpl(config:CouchClientConfigBuilder)(implicit ex:ExecutionContext) extends CouchClient{
  import CouchClient._
  
  private[couchold] val client = new CouchbaseClientExt(config.build)
  addObserver
  private[couchold] val timeoutCounter:OpTimeoutCounter = DefaultOpTimeoutCounter
  val bucket = config.bucket
  val stats = config.statsCollector
  if (config.autoShutdownClient) ShutdownHookThread(shutdown())
  
  //Register operation timing bucket stats
  private[couchold] val stat = BucketStatProto.copy(groupName = config.bucket.name)
  Operations.AllOperations foreach{ op =>    
    stats.registerTimer(stat.copy(key = op))
  }
  
  //Register exception based meter stats  
  stats.registerMeter(stat.copy(key = TimeoutsStat))
  stats.registerMeter(stat.copy(key = ExceptionsStat))
  
  //Register total exception (regular exceps and timeouts) rate stat
  val allCalls = stat.copy(key = Operations.AllOp)
  stats.registerRatioMeters(List(stat.copy(key = TimeoutsStat), stat.copy(key = ExceptionsStat)), List(allCalls), FailureRate)
  
  
  def initialized = true
 
  private[couchold] def addObserver:Unit = client.addObserver(new FunctionBasedConnectionObserver(config.connEstabFuncs, config.connLostFuncs)) 
  
  /**
   * Makes a Promise that will be used for completing later when the callback happens.  The Future from this 
   * Promise will also be chat is returned to the caller so they can receive the asynchronous result
   * @param op A KeyedOperation that will be submitted to the couch connection
   * @param cbFunc A function taking a Promise[PT] and returning a KeyedOperation.  This is used to build
   * the callback that will be submitted to the couch connection
   * @return The Promise that was build to handle the async response from couch
   */
  private def makePromise[PT, RT](op:KeyBasedOperation[_, RT], cbFunc:(Promise[PT]) => KeyedOperation):(Promise[PT], KeyedOperation) = {
    val promise = Promise[PT]
    val cb = cbFunc(promise)
    client.memconn.enqueueOperation(op.key.toString(), cb)
    (promise, cb)
  }
  
  /**
   * Executes a read based operation against couch, returning a Future to obtain the result in an async manner
   * @param op A ReadOperation that will be submitted to the couch server to fetch a record
   * @param timeout An implicit timeout to use for the async callbacks for the future returned
   * @return A Future representing an optional response of type RT
   */
  def <--[RT](op:ReadOperation[_, RT])(implicit timeout:Duration):Future[Option[RT]] = {
    op match{
       
      //Handles requests for empty keys
      case EmptyKeyedOp(o) => Future.failed(new RuntimeException("Empty key supplied for a couchbase operation"))
    
      //If a multi get, execute the multi get specific logic as it's a bit different
      case mg:MultiGet[RT] => multiGet(mg, timeout)
      
      //Else, make a promise for the op and return the future tied to the promise
      case _ => 
        withTimeout[Option[RT]](op){
          makePromise(op, {promise:Promise[Option[RT]] => 
            op.callback(client.opfact, tr => tr.fold(promise.failure(_), promise.success(_)))
          })
        }
    }
  }
  
  /**
   * Adds in timeout handling for the Future that is to be returned
   * @param func A function taking nothing and returning a tuple of type (Promise[RT], KeyedOperation).  The returned
   * value represents the main promise for the operation as well as the operation itself
   * @param timeout An implicit timeout for the operation
   * @return The composite future for either the operation itself or the timeout, whichever completes first
   */
  private[this] def withTimeout[RT](op:Operation[_])(func: => (Promise[RT], KeyedOperation))(implicit timeout:Duration):Future[RT] = {
    val timeoutPromise = Promise[RT]
    val tup = func
    val to = TimeoutScheduler.scheduleTimeout(timeoutPromise, timeout, tup._2, timeoutCounter, client.logger, true, op, this)   
    val fut = tup._1.future
    val start = System.currentTimeMillis
    fut.onComplete{
      case tr => 
        updateTiming(op, start)
        if (tr.isFailure) stats.markMeter(stat.copy(key = ExceptionsStat), 1)
        to.cancel()
        timeoutCounter.success(tup._2)
    }
    Future firstCompletedOf Seq(fut, timeoutPromise.future)    
  }
  
  /**
   * Executes a storage based operation (add, delete, etc...) against couch, returning a Future for
   * obtaining the result of that storage operation in an async manner
   * @param op A StorageOperation to be executed against couch
   * @param timeout An implicit timeout to use for the async callbacks for the future returned
   * @return A Future for obtaining the result of the storage operation
   */
  def -->[T, RT](op:StoreOperation[T, RT])(implicit timeout:Duration):Future[RT] = {
    
    op match{
      //Handles requests for empty keys
      case EmptyKeyedOp(o) => Future.failed(new RuntimeException("Empty key supplied for a couchbase operation"))
      
      case _ =>
        withTimeout[RT](op){
          makePromise(op, { promise:Promise[RT] => 
            op.callback(client.opfact, tr => tr.fold(promise.failure(_), ok => promise.success(ok.get)))
          })
        }        
    }
    
  }

    
  /**
   * Flushes a bucket, using the flush method of the underlying CouchbaseClient
   * @param delay An optional delay time to pass to the flush call, defaulting to no delay
   * @return a Future wrapping a Boolean indicator for the success of the flush
   */
  def flush(delay:Duration = -1 seconds):Future[Boolean] = {
    val promise = Promise[Boolean]
    val flushResult = new AtomicReference[Boolean](true)
    val nodes = nodeLocator.getAll()
    
    val counter = new AtomicInteger(nodes.size())
    client.broadcastOp(new BroadcastOpFactory() {
      def newOp(n:MemcachedNode, latch:CountDownLatch) = {
        val op = client.opfact.flush(delay.toSeconds.toInt, new OperationCallback() {
          def receivedStatus(s:OperationStatus) {
            flushResult.set(s.isSuccess())
          }

          def complete() {
            if (counter.decrementAndGet() == 0){
              promise.complete(Success(flushResult.get))
            }
          }
        })
        op
      }
    })
    
    promise.future   
  }
  
  /**
   * Shuts down the client.
   * @param wait An optional amount of time to wait for the queues to die down before moving forward with the shutdown
   * @return a Boolean representing the result of the shutdown request
   */
  def shutdown(wait:Option[Duration] = None) = {
    val ms = wait.map(dur => dur.toMillis).getOrElse(-1L)
    client.shutdown(ms, TimeUnit.MILLISECONDS)
  }
  
  /**
   * Performs a multi-get against couch for the keys tied to the MultiGet supplied.
   * @param mg A MultiGet operation to be executed against couch
   * @param timeout The amount of time for timeout for the operations
   * @return A Future wrapping an Optional Map of type String,RT.  This map will only contain the keys
   * that were actually found in couch and is not guaranteed to have size = to the keys size supplied
   */
  private def multiGet[RT](mg:MultiGet[RT], timeout:Duration):Future[Option[Map[String, RT]]] = {
    val promise = Promise[Option[Map[String, RT]]]
    
    //Break down the gets into groups by key
    val locator = nodeLocator
    val mappedKeys = mg.keys filter(validateKey) map { key =>
      val primary = locator.getPrimary(key)
      val node = 
        if (primary.isActive()) primary
        else{
          val it = locator.getSequence(key)
          locator.getSequence(key).toList.find (_.isActive()) match{
            case Some(secondary) => secondary
            case None => primary
          }
        }
      (node,key)
    } groupBy (_._1)
    
    //Create the callback to handle the results
    val count = new AtomicInteger(mappedKeys.size)
    val cb = new GetOperation.Callback() {
      var map = Map[String, RT]()
      def receivedStatus(status:OperationStatus ) {
        status match{
          case to:TimedOutOperationStatus if (mg.allowPartial) => promise.complete(Success(Some(map)))          
          case to:TimedOutOperationStatus => promise.failure(new TimeoutException("Operation timed out after " + timeout.toMillis + " millis"))
          case stat if !stat.isSuccess && !mg.allowPartial => promise.failure(UnsuccessfulOperation(mg.toString))
          case _ =>
        }
      }

      def gotData(k:String, flags:Int, data:Array[Byte]) {
        //Only add to the map if no errors decoding the data
        mg.dec(data).foreach{ value =>
          map = map + (k -> value)  
        }        
      }

      def complete() {
        if (count.decrementAndGet() == 0 && !promise.isCompleted) promise.complete(Success(Some(map)))
      }
    }  
    
    //Create the operations to be submitted to the connection
    val fut = promise.future
    val mops = mappedKeys map{ tup =>
      val keys = tup._2 map (_._2)
      val op = client.opfact.get(keys, cb)
      
      //Schedule a timeout for each bulk get to each node
      val tout = TimeoutScheduler.scheduleTimeout(promise, timeout, op, timeoutCounter, client.logger, false, mg, this)
      fut onSuccess {
        case _ => 
          tout.cancel()
          timeoutCounter.success(op)
      }
      
      (tup._1, op)
    } toMap
    
    //TODO: Add a check to the state of the connection here
    client.memconn.addOperations(mops)
    
    //Callback for stats updates
    val start = System.currentTimeMillis
    fut onComplete{
      case tr =>
        updateTiming(mg, start)
        if (tr.isFailure) stats.markMeter(stat.copy(key = ExceptionsStat))
    }
    
    fut
  }
  
  private def updateTiming(op:Operation[_], start:Long) = {
    val time = System.currentTimeMillis - start
    stats.updateTime(stat.copy(key = op.name), time)
    stats.updateTime(stat.copy(key = Operations.AllOp), time)
  }
  
  /**
   * Validates a key, catching the exception and returning false if the key is not valid
   * @param key The key to validate
   * @return A Boolean.  True if valid, false if not
   */
  private def validateKey(key:String):Boolean = 
    try{
      StringUtils.validateKey(key, client.opfact.isInstanceOf[BinaryOperationFactory])
      true
    }
    catch{
      case e:IllegalArgumentException => false
    }
  
  /**
   * Gets the node locator for the couch connection.
   */
  private[couchold] def nodeLocator = client.memconn.getLocator()
}

/**
 * Extension of CouchbaseClient to get at the internal variables used to build and execute operations
 * @param cf The connection factory used for connecting to couch
 */
private[couchold] class CouchbaseClientExt(cf:CouchbaseConnectionFactory) extends CouchbaseClient(cf){
  val memconn = mconn
  val opfact = opFact
  val tcser = tcService
  val trans = transcoder
  val logger = getLogger  
}

private[couchold] object TimeoutScheduler{
  val timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS)
  val TimedOutStatus = new TimedOutOperationStatus
  
  def scheduleTimeout(promise:Promise[_], after:Duration, op:net.spy.memcached.ops.Operation, 
    timeoutCounter:OpTimeoutCounter, logger:Logger, failPromise:Boolean, slipOp:Operation[_], client:CouchClientImpl) = {
    timer.newTimeout(new TimerTask{
      def run(timeout:Timeout){
        timeoutCounter.timeout(op)
        client.stats.markMeter(CouchClient.BucketStatProto.copy(key = CouchClient.TimeoutsStat, groupName = client.bucket.name), 1)
        if (op != null && op.getCallback() != null) op.getCallback().receivedStatus(TimedOutStatus)
        if (failPromise){         
          promise.failure(new TimeoutException("Operation timed out after " + after.toMillis + " millis"))
        }
        val nodeName = op match{
          case value if (value != null && value.getHandlingNode() != null) => op.getHandlingNode().getSocketAddress().toString
          case _ => "<unknown>"
        }
        logger.warn("Timeout on operation, failing node: " + nodeName)        
      }
    }, after.toNanos, TimeUnit.NANOSECONDS)
  }
}

/**
 * Trait defining functions for counting instances of timeouts on operations
 */
private[couchold] trait OpTimeoutCounter{
 
  /**
   * Indicate to the counter that a timeout has occurred
   * @param op The operation that has timed out
   */
  def timeout(op:net.spy.memcached.ops.Operation):Unit
  
  /**
   * Indicate to the timer that an operations succeeded, resetting the rolling timeout count
   * @param op The operation that succeeded
   */
  def success(op:net.spy.memcached.ops.Operation):Unit
}

/**
 * Default impl of OpTimeoutCounter that used the MemcachedConnection functions for op timeout and success
 */
private[couchold] object DefaultOpTimeoutCounter extends OpTimeoutCounter{
  /**
   * {@inheritDoc}
   */
  def timeout(op:net.spy.memcached.ops.Operation) {
    MemcachedConnection.opTimedOut(op)
  }
  
  /**
   * {@inheritDoc}
   */
  def success(op:net.spy.memcached.ops.Operation){
    MemcachedConnection.opSucceeded(op)
  }
}

/**
 * A couch ConnectionObserver class that used a list of connection established and connection lost functions to call when connections are established
 * and lost respectively
 */
private[couchold] class FunctionBasedConnectionObserver(estabFuncs:List[(SocketAddress,Int) => Unit], lostFuncs:List[(SocketAddress) => Unit]) extends ConnectionObserver{
  /**
   * {@inheritDoc}
   */  
  def connectionLost(sa:SocketAddress) = {
    lostFuncs foreach (_(sa))
  }
  
  /**
   * {@inheritDoc}
   */  
  def connectionEstablished(sa:SocketAddress, reconnectCount:Int) = {
    estabFuncs foreach (_(sa,reconnectCount))
  }
}