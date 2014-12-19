package com.aquto.slipstream.couchold
import net.spy.memcached.transcoders.Transcoder
import net.spy.memcached.CachedData
import net.spy.memcached.transcoders.SerializingTranscoder
import scala.util.Try


/**
 * The default function based transcoder to get endode and decode functions from
 * when no explicit encode or decode is supplied for an operation.
 */
object DefaultTranscoder{
  val MaxSize = 1024 * 1024 //Make this configurable or a system prop or somethin
  
  def encode(a:Any):Array[Byte] = SerTranscoder.encode(a).getData()
  
  def decode(bytes:Array[Byte]):Try[Any] = Try(SerTranscoder.decode(new CachedData(0, bytes, MaxSize)))
  
 
  /**
   * A single instance of the serializing transcoder to use for the default decode
   * and encode functions
   */
  private object SerTranscoder extends SerializingTranscoder
}