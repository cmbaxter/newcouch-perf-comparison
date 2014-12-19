package com.aquto.slipstream.couchnew
import scala.util.Try
import scala.util.Success
import com.couchbase.client.java.transcoder.BinaryTranscoder
import com.couchbase.client.deps.io.netty.buffer.ByteBuf
import com.couchbase.client.core.message.ResponseStatus
import com.couchbase.client.java.document.BinaryDocument


/**
 * The default function based transcoder to get endode and decode functions from
 * when no explicit encode or decode is supplied for an operation.
 */
object DefaultTranscoder{
  val MaxSize = 1024 * 1024 //Make this configurable or a system prop or somethin
  
  def encode(a:Any):Array[Byte] = a.toString.getBytes
  
  def decode(bytes:Array[Byte]):Try[Any] = Success(new String(bytes))
}

class AQBinaryTranscoder extends BinaryTranscoder{
  override def doDecode(id:String, content:ByteBuf, cas:Long, expiry:Int, flags:Int, status:ResponseStatus) = {
    BinaryDocument.create(id, expiry, content, cas);
  }
}