package com.aquto.slipstream.load

import java.net.URI
import concurrent.ExecutionContext.Implicits._
import akka.actor._
import akka.event.Logging
import concurrent.duration._
import akka.util.Timeout
import java.util.concurrent.CountDownLatch
import com.aquto.slipstream.couchold.{CouchClient => OldClient, CouchBucket => OldBucket, CouchClientConfigBuilder => OldConfigBuilder, Set => OldSet, Get => OldGet}
import com.aquto.slipstream.couchnew.{CouchClient => NewClient, CouchBucket => NewBucket, CouchClientConfigBuilder => NewConfigBuilder, Set => NewSet, Get => NewGet}
import scala.concurrent.Future

object CouchLoadTest extends App{
  
  //Actor system and implicits needed for the load test
  val system = ActorSystem("couchtest")
  implicit val log = Logging(system.eventStream, "Couch")
  implicit val callTimeout = 2 seconds
  
  //Some toggles to change the behavior of the test
  val loadDuration = 2 minutes
  val useOldClient = false
  val numberOfReqActors = 4
  val couchHost = "load02"
  val bucket = args.headOption.getOrElse("default")
  val bucketPass = args.drop(1).headOption.getOrElse("")
  
  val reaper = system.actorOf(Props[ShutdownReaper])
  val refs =     
    if (useOldClient){
	  val cf = new OldConfigBuilder(List(new URI(s"http://$couchHost:8091/pools")), new OldBucket(bucket, bucketPass))
	  val client = OldClient(cf)  	  
	  for(i <- 1 to numberOfReqActors ) yield {
	    val f = client --> OldSet(i.toString, "foo")
	    concurrent.Await.result(f, callTimeout) //make sure the set done before starting
	    val ref = system.actorOf(Props(classOf[OldRequestActor], i, client, callTimeout))
	    reaper ! ShutdownReaper.WatchActor(ref)
	    ref
	  }
    }
    else{
      val cf = new NewConfigBuilder(List(couchHost), new NewBucket(bucket, bucketPass))
      val client = NewClient(cf, system.eventStream) 
	  for(i <- 1 to numberOfReqActors ) yield {
	    val f = client --> NewSet(i.toString, "foo")
	    concurrent.Await.result(f, callTimeout) //make sure the set done before starting
	    val ref = system.actorOf(Props(classOf[NewRequestActor], i, client, callTimeout))
	    reaper ! ShutdownReaper.WatchActor(ref)
	    ref
	  }      
    }
  
  refs foreach (_ ! "req")
  
  Thread.sleep(loadDuration.toMillis)
  refs foreach(_ ! PoisonPill)
  system.awaitTermination()
  println("system completely shutdown, existing load test")
  Runtime.getRuntime().exit(0)
}

trait RequestHandlingActor extends Actor{
  def receive = {
    case "req" =>               
      val fut = executeRequest
      fut onComplete{
        case any => 
          self ! "req"          
      }              
  }  
  
  def executeRequest:Future[Any]
}

class OldRequestActor(id:Int, client:OldClient, timeout:Duration) extends RequestHandlingActor{
  implicit val _timeout = timeout              
  def executeRequest = client <-- OldGet(id.toString)
}

class NewRequestActor(id:Int, client:NewClient, timeout:Duration) extends RequestHandlingActor{
  implicit val _timeout = timeout              
  def executeRequest = client <-- NewGet(id.toString)
}

object ShutdownReaper{
  case class WatchActor(ref:ActorRef)
}

class ShutdownReaper extends Actor{
  import ShutdownReaper._
  
  var watching = 0
  def receive = {
    case WatchActor(ref) =>
      println(s"watching: $ref")
      watching += 1
      context.watch(ref)
      
    case Terminated(ref) =>
      watching -= 1
      println(s"ref: $ref is terminated, $watching refs remain...")
      if (watching == 0){
        println("shutting down")
        context.system.shutdown
      }
  }
}