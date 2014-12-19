package com.aquto.slipstream.core

/**
 * Event to indicate that an entity was loaded from a sql lookup
 * @param entity The entity that was loaded
 * @param id The unique identifier for that entity
 * @param clazz The class of the entity
 */
case class EntityLoaded(entity:Any, id:Any, clazz:Class[_])

/**
 * Represents a type of Mutation that can occur to an entity
 */
trait MutationType

/**
 * Indicates that an entity was deleted
 */
case object DeleteType extends MutationType

/**
 * Indicates that an entity was updated
 */
case object UpdateType extends MutationType

/**
 * Event to indicate that an entity was mutated (updated, deleted)
 * @param mutationType The type of the mutation that occurred
 * @param id The unique identifier for the entity
 * @param clazz The class of the entity 
 */
case class EntityMutated(mutationType:MutationType, id:Any, clazz:Class[_])