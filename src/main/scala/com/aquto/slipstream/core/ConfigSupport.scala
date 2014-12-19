package com.aquto.slipstream.core

import com.typesafe.config.Config
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions._

/**
 * Trait to provide some functions for dealing with the config of the system
 */
trait ConfigSupport {
  /**
   * Extracts a Duration from the config
   * @param cfg The config to get the Duration from
   * @param prop The name of the duration based prop
   * @return a Duration
   */
  def dur(cfg:Config, prop:String) = Duration(cfg.getDuration(prop, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
  
  /**
   * Checks for the presence of an optional property.  If present, the hasf function is invoked.  If not, the notf function is invoked
   * @param prop The name the prop to check for
   * @param hasf The function to invoke if the prop exists.  
   * @param notf The function to give a default value if the prop does not exist
   * @param cfg The implicit config to use
   * @return an instance of type T
   */
  def opt[T](prop:String, hasf:(String) => T, notf: => T)(implicit cfg:Config):T = if (cfg.hasPath(prop)) hasf(prop) else notf  
  
  /**
   * Gets all of the child property names directly under a root
   * @param root The root to get the child property names for
   * @return a Set of Strings
   */
  def childProperties(root:Config) = {
    for(prop <- root.entrySet()) yield {
      prop.getKey().split("\\.")(0)
    }    
  }
}