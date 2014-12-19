package com.aquto.slipstream.couchold

import scala.concurrent.duration._
import scala.util._
import com.couchbase.client.CouchbaseClient
import net.spy.memcached.OperationFactory
import net.spy.memcached.ops._
import net.spy.memcached.ops.{StoreType => CouchStoreType, StoreOperation => CouchStoreOp}
import net.spy.memcached.CASResponse
import java.util.concurrent.TimeoutException
import language.implicitConversions
import language.postfixOps

object Operations{
  val AllOp = "all"
  val GetOp = "get"
  val GatOp = "gat"
  val GetsOp = "gets"
  val MGetOp = "mget"
  val AddOp = "add"
  val AppendOp = "append"
  val CasOp = "cas"
  val DecrOp = "decr"
  val DeleteOp = "delete"
  val IncrOp = "incr"
  val PrependOp = "prepend"
  val ReplaceOp = "replace"
  val SetOp = "set"
  val TouchOp = "touch"
    
  val AllOperations = List(AllOp, GetOp, GatOp, GetsOp, MGetOp, AddOp, AppendOp, CasOp,
    DecrOp, DeleteOp, IncrOp, PrependOp, ReplaceOp, SetOp, TouchOp)
}

/**
 * Exception class that will be used when a non-successful status code is returned from an operation
 * @param message The message for the error
 */
case class UnsuccessfulOperation(message:String) extends Exception(message)


/**
 * Represents an operation that can be submitted to couch.  The RT type represents
 * the generic return type for the operation
 */
trait Operation[RT] { self: Product =>
  implicit val prod:Product = this
  
  def name:String
  
  private[couchold] def callback(opfact:OperationFactory, completeFunc:(Try[Option[RT]]) => Unit):KeyedOperation
  
  /**
   * Abstract base class for a callback that takes a function to execute on completion
   * @param completeFunc A Function taking an Try[Option[T]] that will be called when the
   * callback completes
   */
  private[couchold] abstract class OpCallback[T](completeFunc:(Try[Option[T]]) => Unit) extends OperationCallback
  
  /**
   * Extension of OpCallback specialized for ops that return a boolean success indicator
   */
  private[couchold] class BooleanOpCallback(completeFunc:(Try[Option[Boolean]]) => Unit) extends OpCallback[Boolean](completeFunc) with CancelledStatusChecking[Boolean]{
    def complete() = {
      val result = checkStatus(prod, stat => Some(stat.isSuccess()))
      completeFunc(result)    
    }
  }  
  
  /**
   * Abstract base class for a read operation based callback that takes a function to execute on completion
   * @param completeFunc A Function taking an Try[Option[T]] that will be called when the
   * callback completes
   */  
  private[couchold] abstract class ReadOpCallback[T](completeFunc:(Try[Option[T]]) => Unit)(implicit p:Product) extends OpCallback(completeFunc){
    var data:Option[T] = None
    var success = true
    def receivedStatus(status:OperationStatus) = {
      success = status match{
        case can:CancelledOperationStatus => false
        case _ => true
      }
    }
    def complete() = completeFunc(if (success) Success(data) else Failure(UnsuccessfulOperation(p.toString)))
  }  
  
  /**
   * Converts a duration into a couchbase expiration seconds value.  This logic also takes into consideration that expirations greater
   * than 30 days are converted into absoulute times based on epoch
   * @param dur The duration to convert
   * @return an Int representing a couchbase based expiration seconds
   */
  implicit def durationToCouchExpirationSeconds(dur:Duration):Int = {
    if (dur > OpDefaults.ThirtyDays){
      ((System.currentTimeMillis + dur.toMillis) / 1000).toInt      
    }
    else 
      dur.toSeconds.toInt
  }
}

/**
 * Extension of Operation for an operation that has a key.  The KT param
 * here represents the type of the key used
 */
trait KeyBasedOperation[KT,RT] extends Operation[RT]{ self:Product =>
  val key:KT
}

/**
 * Extension of KeyBasedOperation for an operation that will be reading data from couch
 */
trait ReadOperation[KT, RT] extends KeyBasedOperation[KT, RT]{ self:Product =>
  
}

/**
 * Extension of ReadOperation for an operation that takes a single String key
 */
trait SingleKeyReadOperation[RT] extends ReadOperation[String, RT]{ self:Product =>
  
}

/**
 * Use this operation to fetch the value associated with a single key.  The return type for a Get corresponds to the return 
 * type from the decoder
 * @params key The key to fetch the value for
 * @params dec An optional decoder function to decoding the bytes returned from couch into an object.  If not
 * supplied then OpDefaults.DefaultDecode will be used.
 */
case class Get[T](key:String, dec:(Array[Byte]) => Try[T] = OpDefaults.DefaultDecode) extends SingleKeyReadOperation[T] {
  
  def name = Operations.GetOp
  
  private[couchold] def callback(opfact:OperationFactory, completeFunc:(Try[Option[T]]) => Unit) = {
    opfact.get(key, new ReadOpCallback(completeFunc) with GetOperation.Callback {
      def gotData(k:String, flags:Int, bytes:Array[Byte]) = {data = dec(bytes).toOption}
    })    
  }
}

/**
 * Composite representation of both the current value for a key and the current cas id for that key
 * @param id The current cas id for a key
 * @param value The current value for that key
 */
case class CasValue[T](id:Long, value:T)

/**
 * Use this operation to perform both a get and touch at the same time, returning a CASValue wrapping the actual
 * value in couch.  The touch will refresh the expiration of the key, keeping it alive for longer. The return type for a Gat is a CASValue wrapping a type
 * that corresponds to the return type from the decoder
 * @params key The key to fetch the value for
 * @param dur A Duration representing how long to keep the key alive for
 * @params dec An optional decoder function to decoding the bytes returned from couch into an object.  If not
 * supplied then OpDefaults.DefaultDecode will be used. 
 */
case class Gat[T](key:String, dur:Duration, dec:(Array[Byte]) => Try[T] = OpDefaults.DefaultDecode) extends SingleKeyReadOperation[CasValue[T]]{
  
  def name = Operations.GatOp
  
  private[couchold] def callback(opfact:OperationFactory, completeFunc:(Try[Option[CasValue[T]]]) => Unit) = {
    opfact.getAndTouch(key, dur, new ReadOpCallback(completeFunc) with GetAndTouchOperation.Callback {
      def gotData(k:String, flags:Int, cas:Long, bytes:Array[Byte]) = data = dec(bytes).toOption.map(CasValue(cas, _))
    })    
  }  
}

/**
 * Use this operation to get the value for a key as well as the CAS information associated with that key.
 * The return type for a Gets is a CASValue wrapping a type that corresponds to the return type from the decoder
 * @params key The key to fetch the value for
 * @params dec An optional decoder function to decoding the bytes returned from couch into an object.  If not
 * supplied then OpDefaults.DefaultDecode will be used. 
 */
case class Gets[T](key:String, dec:(Array[Byte]) => Try[T] = OpDefaults.DefaultDecode) extends SingleKeyReadOperation[CasValue[T]]{
  
  def name = Operations.GetsOp
  
  private[couchold] def callback(opfact:OperationFactory, completeFunc:(Try[Option[CasValue[T]]]) => Unit) = {
    opfact.gets(key, new ReadOpCallback(completeFunc) with GetsOperation.Callback {
      def gotData(k:String, flags:Int, cas:Long, bytes:Array[Byte]) = data = dec(bytes).toOption.map(CasValue(cas, _))
    })
  }
}

/**
 * Use this operation to perform a multi key lookup against couch.  The return type for a MultiGet is a Map of string to value
 * pairings.  Not all keys supplied in the keys param are guaranteed to be in the resulting Map.  The type of value in the Map
 * is determined by the deocder used.
 * @param keys The keys to lookup
 * @param dec An optional decoder function to decoding the bytes returned from couch into an object.  If not
 * supplied then OpDefaults.DefaultDecode will be used.
 * @param allowPartial If set to true and a timeout occurs for some of the results (but not all) then the Future will still be completed successfully
 * with those non-failed results.  If set to false, and there is even a single timeout, the Future will be completed with a failure wrapping a timeout
 * exception.  The default for this param is true. 
 */
case class MultiGet[T](keys:List[String], dec:(Array[Byte]) => Try[T] = OpDefaults.DefaultDecode, allowPartial:Boolean = false) extends ReadOperation[List[String], Map[String, T]]{
  
  def name = Operations.MGetOp
  
  val key = keys
  private[couchold] def callback(opfact:OperationFactory, completeFunc:(Try[Option[Map[String, T]]]) => Unit) = {
    null
  }
}

/**
 * Represents the result of a storage operation, containing a flag indicating success as well as the new cas id of the data
 * @param success Indicates if the storage was successful
 * @param casId An optional cas id returned.  Will be None when the success flag is false
 */
case class StorageResult(success:Boolean, casId:Option[Long])

/**
 * Extension of KeyBasedOperation that represent an operation that will affect the stored data in couch. The T param here
 * represents the type of object being stored
 */
trait StoreOperation[T, RT] extends KeyBasedOperation[String, RT]{ self:Product =>
  
}

/**
 * Enumeration trait for the types of store operations that can be executed
 */
sealed trait StoreType

/**
 * Represents a store operation that will perform a Set against couch.  A Set will create the key if it does not exist and overwrite the
 * key if it does exist
 */
case object SetStore extends StoreType

/**
 * Represents a store operation that will perform an Add against couch.  An Add will only create a new key if it does not already exist.  if
 * the key already existed, nothing will happen and false will be returned as the result.
 */
case object AddStore extends StoreType

/**
 * Represents a store operation that will perform an Replace against couch.  A Replace will only update an existing key.  If the key does not
 * already exist, nothing will happen and false will be returned as the result.
 */
case object ReplaceStore extends StoreType

/**
 * This operation represents a generic storage operation to be executed against couch.  
 * @param storeType The actual store operation (add, set, replace) to execute against couch.
 * @param key The key to store against
 * @param value An instance of type T (derived from the encoder used) to store
 * @param exp The expiration to use for this key
 * @enc An optional encoder operation that will transform the value into a Byte array.  If not supplied, the OpDefaults.DefaultEncode function
 * will be used
 */
private[couchold] case class Store[T](storeType:StoreType, key:String, value:T, exp:Duration, enc:(T) => Array[Byte]) extends StoreOperation[T, StorageResult] { 
  private[couchold] def callback(opfact:OperationFactory, completeFunc:(Try[Option[StorageResult]]) => Unit) = {
    val st = storeType match{
      case SetStore => CouchStoreType.set
      case AddStore => CouchStoreType.add
      case ReplaceStore => CouchStoreType.replace
    }
    opfact.store(st, key, 0, exp, enc(value), new OpCallback[StorageResult](completeFunc) with CouchStoreOp.Callback with CancelledStatusChecking[StorageResult]{
      var cas:Option[Long] = None      
      def gotData(key:String, casId:Long) = {cas = Some(casId)}
      def complete() ={
        val result = checkStatus(prod, stat => Some(StorageResult(stat.isSuccess, cas)))
        completeFunc(result)           
      }
    })
  }  
  
  def name = storeType match{
    case SetStore => Operations.SetOp
    case AddStore => Operations.AddOp
    case ReplaceStore => Operations.ReplaceOp
  }
}

/**
 * Trait to mix into store based operations that will conditionally fail the result if a cancelled operation status
 * is received from the server
 */
private [this] trait CancelledStatusChecking[T]{
  var opStatus:Option[OperationStatus] = None
  def receivedStatus(status:OperationStatus) = {opStatus = Some(status)}
  def checkStatus(prod:Product, ok: OperationStatus => Option[T]):Try[Option[T]] = {
    opStatus match{
      case None => Failure(UnsuccessfulOperation("No status received: " + prod.toString))
      case Some(stat:CancelledOperationStatus) => Failure(UnsuccessfulOperation(prod.toString))
      case Some(stat:TimedOutOperationStatus) => Failure(new TimeoutException("Operation timed out"))
      case Some(stat) => 
        ok(stat) match{
          case None => Failure(UnsuccessfulOperation("Unexpected status received: " + stat))
          case some => Success(some)
        }        
    }            
  }
}

/**
 * Factory object for creating a Store operation that performs a Set against couch
 */
object Set{
  
  /**
   * Creates a Store operation that will perform a Set against couch
   * @param key The key to set against
   * @param value The value of type T to encode and set as the value for the key
   * @param exp An optional duration for when to expire the key.  If not supplied, the key will be set to not expire
   * @param enc An optional encode function to use for turning the value supplied into a Byte array.  If not supplied, then OpDefaults.DefaultEncode will be used 
   */
  def apply[T](key:String, value:T, exp:Duration = OpDefaults.NoExpire, enc:(T) => Array[Byte] = OpDefaults.DefaultEncode) = {
    Store(SetStore, key, value, exp, enc)
  }
}

/**
 * Factory object for creating a Store operation that performs an Add against couch
 */
object Add{
  
  /**
   * Creates a Store operation that will perform an Add against couch
   * @param key The key to add
   * @param value The value of type T to encode and use as the value for the key
   * @param exp An optional duration for when to expire the key.  If not supplied, the key will be set to not expire
   * @param enc An optional encode function to use for turning the value supplied into a Byte array.  If not supplied, then OpDefaults.DefaultEncode will be used 
   */  
  def apply[T](key:String, value:T, exp:Duration = OpDefaults.NoExpire, enc:(T) => Array[Byte] = OpDefaults.DefaultEncode) = {
    Store(AddStore, key, value, exp, enc)
  }
}

/**
 * Factory object for creating a Store operation that performs a Replace against couch
 */
object Replace{
  
  /**
   * Creates a Store operation that will perform a Replace against couch
   * @param key The key to replace the value for
   * @param value The value of type T to encode and use as the value for the key
   * @param exp An optional duration for when to expire the key.  If not supplied, the key will be set to not expire
   * @param enc An optional encode function to use for turning the value supplied into a Byte array.  If not supplied, then OpDefaults.DefaultEncode will be used 
   */    
  def apply[T](key:String, value:T, exp:Duration = OpDefaults.NoExpire, enc:(T) => Array[Byte] = OpDefaults.DefaultEncode) = {
    Store(ReplaceStore, key, value, exp, enc)
  }  
}

/**
 * Represents the type of Mutation to perform when performing a Mutate operation
 */
sealed trait MutationType

/**
 * Used to perform an increment operation against a numeric value
 */
case object Increment extends MutationType

/**
 * Used to perform a decrement operation against a numeric value
 */
case object Decrement extends MutationType

/**
 * This operation will perform an atomic mutation of a numeric value in couch, returning the new value after the mutation
 * @param mutationType The type of atomic mutation to perform (Increment or Decrement)
 * @param key The key to perform the mutation against
 * @param by How much to either increment or decrement by
 * @param default What to set the default value to for the key if the key does not pre-exist
 * @param exp How long the key should live for
 */
private[couchold] case class Mutate(mutationType:MutationType, key:String, by:Long, default:Long, exp:Duration) extends StoreOperation[Unit, Long]{
  private[couchold] def callback(opfact:OperationFactory, completeFunc:(Try[Option[Long]]) => Unit) = {
    opfact.mutate(if (mutationType == Increment) Mutator.incr else Mutator.decr, key, by, default, exp, new OperationCallback() with CancelledStatusChecking[Long]{
      
      def complete() ={
        val result = checkStatus(prod, stat => Some(if (stat.isSuccess()) stat.getMessage().toLong else -1))      
        completeFunc(result)
      }
    })   
  }
  
  def name = mutationType match{
    case Increment => Operations.IncrOp
    case Decrement => Operations.DecrOp
  }
}

/**
 * Factory object for creating a Mutate operation to perform an increment against a numeric key
 */
object Incr{
  
  /**
   * Creates the Mutate for performing an increment against couch
   * @param key The key to perform the increment against
   * @param by An optional value for how much to increment by.  If not supplied, 1 is used.
   * @param default An optional value to use for the value of the key if the key does not pre-exist.  If not supplied, 0 is used.
   * @param exp An optional expiration for how long the key should live for.  If not supplied, the key will not expire
   */
  def apply(key:String, by:Long = 1, default:Long = 0, exp:Duration = OpDefaults.NoExpire) = {
    Mutate(Increment, key, by, default, exp)
  }
}

/**
 * Factory object for creating a Mutate operation to perform an decrement against a numeric key
 */
object Decr{
  
  /**
   * Creates the Mutate for performing an decrement against couch
   * @param key The key to perform the decrement against
   * @param by An optional value for how much to decrement by.  If not supplied, 1 is used.
   * @param default An optional value to use for the value of the key if the key does not pre-exist.  If not supplied, 0 is used.
   * @param exp An optional expiration for how long the key should live for.  If not supplied, the key will not expire
   */  
  def apply(key:String, by:Long = 1, default:Long = 0, exp:Duration = OpDefaults.NoExpire) = {
    Mutate(Decrement, key, by, default, exp)
  }
}

/**
 * Represents the result of a Cas operation.  Can be a successful result (CasSuccess) of a failed update (CasFailure)
 */
trait CasResult

/**
 * Represents a type of success that can happen as a result of a Cas operation
 */
trait CasSuccessType

/**
 * Represents a successful update of a key via Cas
 */
case object Ok extends CasSuccessType

/**
 * Represents a successfully completed Cas operation
 * @param successType The type of success that occurred
 */
case class CasSuccess(successType:CasSuccessType) extends CasResult

/**
 * Represents a failed attempt to update a key via a Cas operation
 */
trait CasFailureType

/**
 * Represents the situation where the cas id supplied is no longer the current cas id for the key
 */
case object Contention extends CasFailureType

/**
 * Represents the situation where the key to update no longer exists
 */
case object NoKeyFound extends CasFailureType

/**
 * Represents a cas failure based on observing a key
 */
case object ObserveError extends CasFailureType

/**
 * Represents a failed cas operation
 * @param failType The type of failure that occurred
 */
case class CasFailure(failType:CasFailureType) extends CasResult

/**
 * Use this to perform a CAS (check and set) operation against couch.
 * @param key The key to perform the CAS against
 * @param casId The expected cas id of the key in couch.  If there is a mismatch, the CAS will fail.
 * @param value The new value to use for the key.  The type T here is determined by the encode function used
 * @param exp An optional expiration for the key.  If not supplied, the key will not expire.
 * @param enc An optional encode function to turn the value into a Byte array.  If not supplied, OpDefaults.DefaultEncode will be used
 */
case class Cas[T](key:String, casId:Long, value:T, exp:Duration = OpDefaults.NoExpire, enc:(T) => Array[Byte] = OpDefaults.DefaultEncode) 
  extends StoreOperation[T, CasResult] {
  
  def name = Operations.CasOp
  
  private[couchold] def callback(opfact:OperationFactory, completeFunc:(Try[Option[CasResult]]) => Unit) = {
    opfact.cas(CouchStoreType.set, key, casId, 0, exp, enc(value), new CouchStoreOp.Callback() with CancelledStatusChecking[CasResult]{
      def gotData(key:String, cas:Long) = {}      
      def checkCasStatus(status:OperationStatus) = {
        status match{
          case casStat:CASOperationStatus => 
            val casResult = casStat.getCASResponse() match{
              case CASResponse.OK => CasSuccess(Ok)
              case CASResponse.EXISTS => CasFailure(Contention)
              case CASResponse.NOT_FOUND => CasFailure(NoKeyFound)
              case _ => CasFailure(ObserveError)
            }
            Some(casResult)
          case _ => None
        }
      }
      
      def complete() = {
        val result = checkStatus(prod, checkCasStatus(_))        
        completeFunc(result)
      }
    })     
  }
}

/**
 * Enumeration type for the type of a concatenation operation to perform
 */
sealed trait ConcatType

/**
 * Represents a concat operation that performs an append to a pre-exsting value
 */
case object AppendType extends ConcatType

/**
 * Represents a concat operation that performs a prepend to a pre-existing value
 */
case object PrependType extends ConcatType

/**
 * Use this operation to perform a concatenation operation against the value for a pre-existing key in couch
 * @param concatType The type of concatenation (append, prepend) to perform
 * @param key The key to perform the concat against
 * @param casId The expected cas identifier for the value to update.  The concat will fail if the expected does not match the current cas id.
 */
private[couchold] case class Concat[T](concatType:ConcatType, key:String, casId:Long, value:T, enc:(T) => Array[Byte]) extends StoreOperation[T, Boolean]{
  private[couchold] def callback(opfact:OperationFactory, completeFunc:(Try[Option[Boolean]]) => Unit) = {
    opfact.cat(if (concatType == AppendType) ConcatenationType.append else ConcatenationType.prepend, casId, key, enc(value), new BooleanOpCallback(completeFunc))
  }
  
  def name = concatType match{
    case AppendType => Operations.AppendOp
    case PrependType => Operations.PrependOp
  }
}

/**
 * Factory object for performing an Append concatenation against couch
 */
object Append{
  
  /**
   * Creates a Concat operation for performing an Append
   * @param key The key to append against
   * @param casId The expected cas id
   * @param value The value to append.  The type here is determined by the encode operation supplied
   * @param enc An optional encode operation that will convert the value supplied into a Byte array.  If not supplied, OpDefaults.DefaultEncode will be used
   */
  def apply[T](key:String, casId:Long, value:T, enc:(T) => Array[Byte] = OpDefaults.DefaultEncode) = Concat[T](AppendType, key, casId, value, enc)
}

/**
 * Factory object for performing an Prepend concatenation against couch
 */
object Prepend{
  
  /**
   * Creates a Concat operation for performing a Prepend
   * @param key The key to prepend against
   * @param casId The expected cas id
   * @param value The value to prepend.  The type here is determined by the encode operation supplied
   * @param enc An optional encode operation that will convert the value supplied into a Byte array.  If not supplied, OpDefaults.DefaultEncode will be used
   */
  def apply[T](key:String, casId:Long, value:T, enc:(T) => Array[Byte] = OpDefaults.DefaultEncode) = Concat[T](PrependType, key, casId, value, enc)
}

/**
 * Use this operation to perform a Touch operation against couch.  This will extend the life of a pre-existing key
 * @param key The key to extend the life for
 * @param dur The duration to extend the life for
 */
case class Touch(key:String, dur:Duration) extends StoreOperation[Unit,Boolean]{
  private[couchold] def callback(opfact:OperationFactory, completeFunc:(Try[Option[Boolean]]) => Unit) = {
    opfact.touch(key, dur, new BooleanOpCallback(completeFunc))
  }
  
  def name = Operations.TouchOp
}

/**
 * Use this operation to remove a pre-existing key from couch
 * @param key The key to remove from couch
 */
case class Delete(key:String) extends StoreOperation[Unit, Boolean]{
  private[couchold] def callback(opfact:OperationFactory, completeFunc:(Try[Option[Boolean]]) => Unit) = {
    opfact.delete(key, new BooleanOpCallback(completeFunc) with DeleteOperation.Callback{
      def gotData(cas:Long) = {}
    })
  }  
  
  def name = Operations.DeleteOp
}

/**
 * Object for holding some defaults related to the operations that can be executed against couch
 */
object OpDefaults{
  
  /** The default encode function to use for submitting values to couch.  Uses the encoder property from DefaultTranscoder (which uses basic object serialization to encode) */
  val DefaultEncode = DefaultTranscoder.encode _
  
  /** The default decode function to use for reading values returned by couch.  Uses the decoder property from DefaultTranscoder (which uses basic object serialization to decode) */
  val DefaultDecode = DefaultTranscoder.decode _
  val NoExpire = 0 seconds
  val ThirtyDays = 30 days
}