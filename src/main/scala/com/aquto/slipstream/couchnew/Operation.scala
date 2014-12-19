package com.aquto.slipstream.couchnew

import scala.concurrent.duration._
import scala.util._
import java.util.concurrent.TimeoutException
import language.implicitConversions
import language.postfixOps
import com.couchbase.client.java.AsyncBucket
import rx.lang.scala.Observable
import rx.lang.scala.JavaConversions._
import com.couchbase.client.java.document.LegacyDocument
import com.couchbase.client.java.document.BinaryDocument
import com.couchbase.client.deps.io.netty.buffer.ByteBuf
import com.couchbase.client.deps.io.netty.buffer.Unpooled
import com.couchbase.client.java.document.JsonLongDocument
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.error.DocumentDoesNotExistException
import com.couchbase.client.java.error.CASMismatchException
import com.couchbase.client.java.view.ViewQuery
import com.couchbase.client.java.view.Stale
import com.couchbase.client.java.view.AsyncViewResult
import com.couchbase.client.java.view.View
import com.couchbase.client.java.view.DefaultView
import com.couchbase.client.java.view.{DesignDocument => APIDesignDoc}
import com.couchbase.client.java.bucket.AsyncBucketManager

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
  val QueryViewOp = "queryView"
  val QueryViewDocsOp = "queryViewDocs"
  val UpdateDesignDocOp = "updateDesignDoc"
    
  val AllOperations = List(AllOp, GetOp, GatOp, GetsOp, MGetOp, AddOp, AppendOp, CasOp,
    DecrOp, DeleteOp, IncrOp, PrependOp, ReplaceOp, SetOp, TouchOp, QueryViewOp, QueryViewDocsOp, UpdateDesignDocOp)
  val BinDocClass = classOf[BinaryDocument]
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
trait Operation[RT] {
  def name:String
  
  def currentTime = System.currentTimeMillis()
  
  /**
   * Converts a duration into a couchbase expiration seconds value.  This logic also takes into consideration that expirations greater
   * than 30 days are converted into absoulute times based on epoch
   * @param dur The duration to convert
   * @return an Int representing a couchbase based expiration seconds
   */
  implicit def durationToCouchExpirationSeconds(dur:Duration):Int = {
    if (dur > OpDefaults.ThirtyDays){
      ((currentTime + dur.toMillis) / 1000).toInt            
    }
    else 
      dur.toSeconds.toInt
  }
    
}

/**
 * Extension of KeyBasedOperation for an operation that will be reading data from couch
 */
trait ReadOperation[RT] extends Operation[RT]{  
  private[couchnew] def lookup(key:String, bucket:AsyncBucket):Observable[BinaryDocument] = bucket.get(key, Operations.BinDocClass)
  private[couchnew] def execute(bucket:AsyncBucket):Observable[RT]
  private[couchnew] def decode[T](doc:BinaryDocument, dec:(Array[Byte]) => Try[T], success:T => RT):Observable[RT] = {
    dec(doc.content.array) match{
      case util.Failure(ex) => 
        //Might want to log this, but for now a decoding error results in an empty observable
        Observable.empty
        
      case util.Success(result) => Observable.just(success(result))
    }
  }
}

/**
 * Use this operation to fetch the value associated with a single key.  The return type for a Get corresponds to the return 
 * type from the decoder
 * @params key The key to fetch the value for
 * @params dec An optional decoder function to decoding the bytes returned from couch into an object.  If not
 * supplied then OpDefaults.DefaultDecode will be used.
 */
case class Get[T](key:String, dec:(Array[Byte]) => Try[T] = OpDefaults.DefaultDecode) extends ReadOperation[T]{
  def name = Operations.GetOp
  private[couchnew] def execute(bucket:AsyncBucket):Observable[T] = {
    lookup(key, bucket).flatMap(decode[T](_, dec, identity))
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
case class Gat[T](key:String, dur:Duration, dec:(Array[Byte]) => Try[T] = OpDefaults.DefaultDecode) extends ReadOperation[CasValue[T]]{
  def name = Operations.GatOp
  private[couchnew] def execute(bucket:AsyncBucket):Observable[CasValue[T]] = {
    val obs:Observable[BinaryDocument] = bucket.getAndTouch(key, dur, Operations.BinDocClass )
    obs.flatMap(d => decode[T](d, dec, t => CasValue(d.cas, t)))
  }  
}

/**
 * Use this operation to get the value for a key as well as the CAS information associated with that key.
 * The return type for a Gets is a CASValue wrapping a type that corresponds to the return type from the decoder
 * @params key The key to fetch the value for
 * @params dec An optional decoder function to decoding the bytes returned from couch into an object.  If not
 * supplied then OpDefaults.DefaultDecode will be used. 
 */
case class Gets[T](key:String, dec:(Array[Byte]) => Try[T] = OpDefaults.DefaultDecode) extends ReadOperation[CasValue[T]]{
  def name = Operations.GetsOp
  private[couchnew] def execute(bucket:AsyncBucket):Observable[CasValue[T]] = {
    lookup(key, bucket).flatMap(d => decode[T](d, dec, t => CasValue(d.cas, t)))
  }  
}

/**
 * Use this operation to perform a multi key lookup against couch.  The return type for a MultiGet is a Map of string to value
 * pairings.  Not all keys supplied in the keys param are guaranteed to be in the resulting Map.  The type of value in the Map
 * is determined by the deocder used.
 * @param keys The keys to lookup
 * @param dec An optional decoder function to decoding the bytes returned from couch into an object.  If not
 * supplied then OpDefaults.DefaultDecode will be used.
 * @param allowPartial - Set this to true if you can tolerate errors with the lookup of some keys or timeouts with the lookups of some keys.  Set to false if can not
 * tolerate any errors/timeouts on keys.
 * exception.  The default for this param is true. 
 */
case class MultiGet[T](keys:List[String], dec:(Array[Byte]) => Try[T] = OpDefaults.DefaultDecode, allowPartial:Boolean = true) extends ReadOperation[Map[String, T]]{
  def name = Operations.MGetOp 
  private[couchnew] def execute(bucket:AsyncBucket):Observable[Map[String, T]] = {
    
    def decodeToTuple(doc:BinaryDocument) = dec(doc.content().array).toOption.map((doc.id, _))
    
    Observable.
      from(keys).
      flatMap{id => 
        val obs = bucket.get(id, classOf[BinaryDocument])
        
        //If we are being lenient and allowing partial, swallow up any errors.  If not, leave the observable as is
        if (allowPartial) obs.onErrorResumeNext(rx.Observable.empty)
        else obs
      }.
      toList.
      map(_.flatMap(decodeToTuple).toMap)    
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
trait StoreOperation[T, RT] extends Operation[RT]{
  private[couchnew] def toByteBuf(bytes:Array[Byte]):ByteBuf = Unpooled.copiedBuffer(bytes)
  private[couchnew] def execute(bucket:AsyncBucket):Observable[RT]
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
case class Store[T](storeType:StoreType, key:String, value:T, exp:Duration, enc:(T) => Array[Byte]) extends StoreOperation[T, StorageResult]{
  private[couchnew] def execute(bucket:AsyncBucket):Observable[StorageResult] = {
    val encoded = toByteBuf(enc(value))
    val doc = BinaryDocument.create(key, exp, encoded)
    val obs:Observable[BinaryDocument] = storeType match{
      case SetStore => bucket.upsert(doc)
      case AddStore => bucket.insert(doc)
      case ReplaceStore => bucket.replace(doc)
    }
    obs.
      map(d => StorageResult(true, Some(d.cas))).
      onErrorReturn(ex => StorageResult(false, None))
  }
  
  def name = storeType match{		
    case SetStore => Operations.SetOp		
    case AddStore => Operations.AddOp		
    case ReplaceStore => Operations.ReplaceOp		
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
private[couchnew] case class Mutate(mutationType:MutationType, key:String, by:Long, default:Long, exp:Duration) extends StoreOperation[Long, Long]{
  private[couchnew] def execute(bucket:AsyncBucket):Observable[Long] = {
    val byVal = mutationType match{
      case Increment => by
      case Decrement => by * -1
    }
    val obs:Observable[JsonLongDocument] = bucket.counter(key, byVal, default, exp)
    obs.map(_.content())
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
   * @param default An optional value to use for the value of the key if the key does not pre-exist.  If not supplied, 1 is used.
   * @param exp An optional expiration for how long the key should live for.  If not supplied, the key will not expire
   */
  def apply(key:String, by:Long = 1, default:Long = 1, exp:Duration = OpDefaults.NoExpire) = {
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
   * @param default An optional value to use for the value of the key if the key does not pre-exist.  If not supplied, 1 is used.
   * @param exp An optional expiration for how long the key should live for.  If not supplied, the key will not expire
   */  
  def apply(key:String, by:Long = 1, default:Long = 1, exp:Duration = OpDefaults.NoExpire) = {
    Mutate(Decrement, key, by, default, exp)
  }
}

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
 * Represents the result of a Cas operation
 */
case class CasResult(success:Boolean, failType:Option[CasFailureType] = None)

/**
 * Use this to perform a CAS (check and set) operation against couch.
 * @param key The key to perform the CAS against
 * @param casId The expected cas id of the key in couch.  If there is a mismatch, the CAS will fail.
 * @param value The new value to use for the key.  The type T here is determined by the encode function used
 * @param exp An optional expiration for the key.  If not supplied, the key will not expire.
 * @param enc An optional encode function to turn the value into a Byte array.  If not supplied, OpDefaults.DefaultEncode will be used
 */
case class Cas[T](key:String, casId:Long, value:T, exp:Duration = OpDefaults.NoExpire, enc:(T) => Array[Byte] = OpDefaults.DefaultEncode) extends StoreOperation[T, CasResult]{
  def name = Operations.CasOp
  private[couchnew] def execute(bucket:AsyncBucket):Observable[CasResult] = {
    val bbuf = toByteBuf(enc(value))
    val doc = BinaryDocument.create(key, exp, bbuf, casId)
    val obs:Observable[BinaryDocument] = bucket.replace(doc)
    obs.
      map(d => CasResult(true)).
      onErrorReturn{
        case ex:DocumentDoesNotExistException => CasResult(false, Some(NoKeyFound))
        case other => CasResult(false, Some(Contention))
      }
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
 */
private[couchnew] case class Concat[T](concatType:ConcatType, key:String, value:T, enc:(T) => Array[Byte]) extends StoreOperation[T, Boolean]{
  private[couchnew] def execute(bucket:AsyncBucket):Observable[Boolean] = {
    val bbuf = toByteBuf(enc(value))
    val doc = BinaryDocument.create(key, bbuf)
    val obs:Observable[BinaryDocument] = concatType match{
      case AppendType => bucket.append(doc)
      case PrependType => bucket.prepend(doc)
    }
    obs.
      map(d => true).
      onErrorReturn(ex => false)
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
   * @param value The value to append.  The type here is determined by the encode operation supplied
   * @param enc An optional encode operation that will convert the value supplied into a Byte array.  If not supplied, OpDefaults.DefaultEncode will be used
   */
  def apply[T](key:String, value:T, enc:(T) => Array[Byte] = OpDefaults.DefaultEncode) = Concat[T](AppendType, key, value, enc)
}

/**
 * Factory object for performing an Prepend concatenation against couch
 */
object Prepend{
  
  /**
   * Creates a Concat operation for performing a Prepend
   * @param key The key to prepend against
   * @param value The value to prepend.  The type here is determined by the encode operation supplied
   * @param enc An optional encode operation that will convert the value supplied into a Byte array.  If not supplied, OpDefaults.DefaultEncode will be used
   */
  def apply[T](key:String, value:T, enc:(T) => Array[Byte] = OpDefaults.DefaultEncode) = Concat[T](PrependType, key, value, enc)
}

/**
 * Use this operation to perform a Touch operation against couch.  This will extend the life of a pre-existing key
 * @param key The key to extend the life for
 * @param dur The duration to extend the life for
 */
case class Touch(key:String, dur:Duration) extends StoreOperation[Unit,Boolean]{
  def name = Operations.TouchOp 
  private[couchnew] def execute(bucket:AsyncBucket):Observable[Boolean] = {
    val obs:Observable[java.lang.Boolean] = bucket.touch(key, dur)
    obs.
      map(_.booleanValue).
      onErrorReturn(ex => false)
  }
}

/**
 * Use this operation to remove a pre-existing key from couch
 * @param key The key to remove from couch
 */
case class Delete(key:String) extends StoreOperation[Unit, Boolean]{
  def name = Operations.DeleteOp 
  private[couchnew] def execute(bucket:AsyncBucket):Observable[Boolean] = {
    val obs:Observable[JsonDocument] = bucket.remove(key)
    obs.
      map(d => true).
      onErrorReturn(ex => false)
  }
}

/**
 * Companion to the QueryView class 
 */
object QueryView{
  sealed trait Staleness
  object Staleness{
    case object Ok extends Staleness
    case object False extends Staleness
    case object UpdateAfter extends Staleness
  }
  
  case class KeyFuncs(intF:Int => ViewQuery, longF:Long => ViewQuery, doubF:Double => ViewQuery, boolF:Boolean => ViewQuery, stringF:String => ViewQuery)
  
  implicit class QueryPimp(val query:ViewQuery) extends AnyVal{
    def opt[T](option:Option[T])(f:ViewQuery => T => ViewQuery):ViewQuery = {
      option.fold(query)(t => f(query).apply(t))
    } 
    def keyOpt[T](option:Option[T])(f:ViewQuery => KeyFuncs):ViewQuery = {
      val funcs = f(query)
      option.fold(query){ 
        case i:Int => funcs.intF(i)
        case l:Long => funcs.longF(l)
        case d:Double => funcs.doubF(d)
        case b:Boolean => funcs.boolF(b)
        case other => funcs.stringF(other.toString)
      }
    }     
  }
}


/**
 * Represents an individual row result from a view query
 */
case class ViewRowResult(id:String, value:Option[String])

/**
 * Represents the set of results from a view query
 */
case class ViewQueryResults(totalRows:Int, rows:List[ViewRowResult])

/**
 * Trait to mix into operations that perform view queries
 */
trait ViewQuerying{
  import QueryView._
  
  /**
   * Does the work of building out the ViewQuery object and making the request to the view.  Also flatMaps the observable
   * to a failed one if the result of the query was not successful
   */
  def queryView(request:QueryView, bucket:AsyncBucket):Observable[AsyncViewResult] = {
    val stale = request.staleness match{
      case Staleness.Ok => Stale.TRUE
      case Staleness.False => Stale.FALSE
      case Staleness.UpdateAfter => Stale.UPDATE_AFTER
    }
    
    //Build out the base query from the required info
    val baseQuery = ViewQuery.
      from(request.designDoc, request.view).
      stale(stale)
      
    //Layer in the optional info via the pimped in 'opt' method
    val query = baseQuery.
      keyOpt(request.key){ vq =>
        QueryView.KeyFuncs(vq.key, vq.key, vq.key, vq.key, vq.key)
      }.      
      keyOpt(request.startKey){ vq =>
        QueryView.KeyFuncs(vq.startKey, vq.startKey, vq.startKey, vq.startKey, vq.startKey)
      }.
      keyOpt(request.endKey){ vq =>
        QueryView.KeyFuncs(vq.endKey, vq.endKey, vq.endKey, vq.endKey, vq.endKey)
      }.
      opt(request.limit)(_.limit).
      opt(request.skip)(_.skip).
      opt(request.descending)(_.descending).
      opt(request.inclusiveEnd)(_.inclusiveEnd)      
      
    //Make the query and flatMap to a failed observable if the result was not successful
    val obs:Observable[AsyncViewResult] = bucket.query(query) 
    obs.flatMap{ result =>
      if (result.success()) Observable.just(result)
      else Observable.error(new RuntimeException(s"Query of '$query' to view '${request.view}' for doc '${request.designDoc}' was not successful"))
    }
  }
}

/**
 * Represents a request to query a view.  This request will not load the underlying docs and will instead just return the raw view query results.
 * If you want to load the underlying documents also, use QueryViewDocuments instead.
 */
case class QueryView(
  designDoc:String, 
  view:String, 
  staleness:QueryView.Staleness, 
  key:Option[Any] = None, 
  startKey:Option[Any] = None, 
  endKey:Option[Any] = None, 
  limit:Option[Int] = None, 
  skip:Option[Int] = None,
  descending:Option[Boolean] = None,
  inclusiveEnd:Option[Boolean] = None
) extends ReadOperation[ViewQueryResults] with ViewQuerying{  
  
  def name = Operations.QueryViewOp 
  
  def execute(bucket:AsyncBucket):Observable[ViewQueryResults] = {    
    val obs = queryView(this, bucket)
    obs.              
      flatMap(_.rows).
      map(r => ViewRowResult(r.id(), Option(r.value).map(_.toString))).
      toList.
      map(rows => ViewQueryResults(rows.size, rows))        
  } 
}

/**
 * Companion to the QueryViewDocuments class
 */
case object QueryViewDocuments{
  
  /**
   * Default parseFunc for a QueryViewDocuments request.  Returns the json as is (a string)
   * @param json The input json
   * @return a String (the json as is)
   */
  def asIs(json:String):String = json
}

/**
 * Represents a request to perform a view query and load the corresponding documents tied to the keys for the view results.  Allows the caller
 * to supply a parsing function to convert the json into the type T.  If not explicitly supplied, this will default to being the json as is (a string)
 */
case class QueryViewDocuments[T](query:QueryView, parseFunc:String => T = QueryViewDocuments.asIs _) extends ReadOperation[Map[String,T]] with ViewQuerying{
  def name = Operations.QueryViewDocsOp 
  
  def execute(bucket:AsyncBucket):Observable[Map[String,T]] = {
    val obs = queryView(query, bucket)
    obs.
      flatMap(_.rows).
      flatMap(_.document(classOf[BinaryDocument]).onExceptionResumeNext(rx.Observable.empty[BinaryDocument])). //Add logging for this error swallowing???
      toList.
      map{ docs => 
        docs.map{ d=> 
          val json = new String(d.content().array())
          (d.id, parseFunc(json))
        }(collection.breakOut)
      }
  }
}

/**
 * Represents a View that is part of a design document
 */
case class DesignDocView(name:String, mapFunction:String, reduceFunction:Option[String] = None)

/**
 * Represents a design document that is attached to a bucket and contains queryable views
 */
case class DesignDocument(name:String, views:List[DesignDocView])


/**
 * Represents a request to update (upsert) a Design Document into the system and attach it to a bucket
 */
case class UpdateDesignDocument(doc:DesignDocument) extends StoreOperation[Unit,Boolean]{
  import collection.JavaConversions._
  def name = Operations.UpdateDesignDocOp 
  
  def execute(bucket:AsyncBucket):Observable[Boolean] = {
    val views = doc.views.map(v => DefaultView.create(v.name, v.mapFunction, v.reduceFunction.getOrElse(null)))
    val apidoc = APIDesignDoc.create(doc.name, views)
    val obs:Observable[AsyncBucketManager] = bucket.bucketManager()
    obs.
      flatMap(_.upsertDesignDocument(apidoc)).
      map(_ => true)
  }
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