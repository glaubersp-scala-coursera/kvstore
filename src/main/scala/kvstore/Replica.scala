package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Kill, PoisonPill, Props}
import akka.event.LoggingReceive
import kvstore.Arbiter._

object Replica {

  sealed trait Operation {
    def key: String

    def id: Long
  }

  case class Insert(key: String, value: String, id: Long) extends Operation

  case class Remove(key: String, id: Long) extends Operation

  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply

  case class OperationAck(id: Long) extends OperationReply

  case class OperationFailed(id: Long) extends OperationReply

  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props =
    Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {

  import Persistence._
  import Replica._
  import Replicator._
  import akka.actor.OneForOneStrategy
  import akka.actor.SupervisorStrategy._
  import context.dispatcher

  import scala.concurrent.duration._
  import scala.language.postfixOps

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  // ================  REPLICATION ===================
  var _replicateIdCounter = 0L

  def nextReplicateId(): Long = {
    val ret = _replicateIdCounter
    _replicateIdCounter += 1
    ret
  }

  // Track Replicate Message Ids to all Replicators
  var replicateMessageIdToReplicatorsAndQuantityMap = Map.empty[Long, Map[ActorRef, Int]]

  def trackReplicateMessageIdToReplicator(msgId: Long, replicator: ActorRef): Unit = {
    var replicatorToSentMessagesMap = Map.empty[ActorRef, Int]
    if (replicateMessageIdToReplicatorsAndQuantityMap.get(msgId).isDefined) {
      replicatorToSentMessagesMap = replicateMessageIdToReplicatorsAndQuantityMap(msgId)
      var quantity = 0
      if (replicatorToSentMessagesMap.get(replicator).isDefined) {
        quantity = replicatorToSentMessagesMap(replicator)
      }
      replicatorToSentMessagesMap += (replicator -> (quantity + 1))
    } else {
      replicatorToSentMessagesMap += (replicator -> 1)
    }
    replicateMessageIdToReplicatorsAndQuantityMap += (msgId -> replicatorToSentMessagesMap)
  }

  // Track Replicate message id to its sender
  var toBeAckedMessageIdToSenderMap = Map.empty[Long, ActorRef]

  def trackToBeAckedMessageIdToSender(msgId: Long, sender: ActorRef): Unit = {
    toBeAckedMessageIdToSenderMap += (msgId -> sender)
  }

  var replicationCancellables = Map.empty[Long, Cancellable]
  // ================ END OF REPLICATION ===================

  // ================ PERSISTENCE ===================

  // The Persistence actor
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 second) {
      case _: Exception => Restart
    }
  val persistencer = context.actorOf(persistenceProps)

  // Track Persistence Message id to Persistencer
  var persistenceMessageIdToPersistencerMap = Map.empty[Long, Map[ActorRef, Int]]

  def trackPersistenceMessageIdToPersistencer(msgId: Long, persistencer: ActorRef): Unit = {
    var persistencerToQuantityMap = Map.empty[ActorRef, Int]
    if (persistenceMessageIdToPersistencerMap.get(msgId).isDefined) {
      persistencerToQuantityMap = persistenceMessageIdToPersistencerMap(msgId)
      var quantity = 0
      if (persistencerToQuantityMap.get(persistencer).isDefined) {
        quantity = persistencerToQuantityMap(persistencer)
      }
      persistencerToQuantityMap += (persistencer -> (quantity + 1))
    } else {
      persistencerToQuantityMap += (persistencer -> 1)
    }
    persistenceMessageIdToPersistencerMap += (msgId -> persistencerToQuantityMap)
  }

  // Track Persistence message id to sender
  var persistenceMessageIdToSenderMap = Map.empty[Long, ActorRef]

  def trackPersistenceMessageIdToSender(msgId: Long, sender: ActorRef): Unit = {
    persistenceMessageIdToSenderMap += (msgId -> sender)
  }

  var persistenceCancellables = Map.empty[Long, Cancellable]
  // ================ END OF PERSISTENCE ===================

  // ================ OPERATION ACK/FAIL ===================
  var ackCancellables = Map.empty[Long, Cancellable]

  // Track Ack/Fail Message Ids to sender
  var ackMessageIdToQuantityMap = Map.empty[Long, Int]

  def trackAckMessageIdToSender(msgId: Long): Unit = {
    var quantity = 1
    if (ackMessageIdToQuantityMap.get(msgId).isDefined) {
      quantity += ackMessageIdToQuantityMap(msgId)
    }
    ackMessageIdToQuantityMap += (msgId -> quantity)
  }

  // ================ END OF OPERATION ACK/FAIL ===================

  var _snapshotAckCounter = 0L

  def expectedSnapshotAckId(): Long = {
    _snapshotAckCounter
  }

  def setExpectedSnapshotAckId(lastAckId: Long): Unit = {
    _snapshotAckCounter = _snapshotAckCounter max (lastAckId + 1)
  }

  def receive: Receive = LoggingReceive {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* Behavior for  the leader role. */
  val leader: Receive = LoggingReceive {

    case Insert(key, value, id) =>
      kv += (key -> value)

      // Keep sender of message for Ack purpose
      trackToBeAckedMessageIdToSender(id, sender())

      // Replicate to secondaries
      replicators.foreach(replicator => {
        trackReplicateMessageIdToReplicator(id, replicator)
        log.debug(s"SEND: $replicator ! Replicate($key, Some($value), $id)")
        replicator ! Replicate(key, Some(value), id)

      })

      // Schedule to resend Replicate messages
      val cancellable: Cancellable = context.system.scheduler.schedule(100 millisecond, 100 milliseconds) {
        if (replicateMessageIdToReplicatorsAndQuantityMap.get(id).isDefined) {
          // Replicate may be not confirmed. Check and resend to each replicator.
          val replicatorsAndQuantityMap = replicateMessageIdToReplicatorsAndQuantityMap(id)
          for (replicator <- replicatorsAndQuantityMap.keySet) {
            val times = replicatorsAndQuantityMap(replicator)
            if (times < 10) {
              trackReplicateMessageIdToReplicator(id, replicator)
              log.debug(s"RE-SEND: $replicator ! Replicate($key, $Some(value), $id)")
              replicator ! Replicate(key, Some(value), id)
            } else {
              // Generate OperationFailed message to the original sender
              log.debug(s"FAIL-INSERT-REPLICATE: ${toBeAckedMessageIdToSenderMap(id)} ! OperationFailed($id)")
              toBeAckedMessageIdToSenderMap(id) ! OperationFailed(id)
            }
          }
        } else {
          // Replicate already confirmed. Cancel resend.
          log.debug(s"CANCEL: Replicate($key, $Some(value), $id)")
          replicationCancellables(id).cancel()
          replicationCancellables -= id
        }
      }
      // Keep track of generated cancellable to the Replicate messages
      replicationCancellables += (id -> cancellable)

      // Persist
      trackPersistenceMessageIdToPersistencer(id, persistencer)
      trackPersistenceMessageIdToSender(id, sender())
      log.debug(s"SEND: $persistencer ! Persist($key, Some($value), $id)")
      persistencer ! Persist(key, Some(value), id)
      // Schedule to resend Persist message
      val cancellablePersistence: Cancellable = context.system.scheduler.schedule(100 millisecond, 100 milliseconds) {
        if (persistenceMessageIdToPersistencerMap.get(id).isDefined) {
          val persistenceToQuantityMap = persistenceMessageIdToPersistencerMap(id)
          for (actor <- persistenceToQuantityMap.keySet) {
            val times = persistenceToQuantityMap(actor)
            if (times < 10) {
              // Persistence not confirmed. Resend Persist
              trackPersistenceMessageIdToPersistencer(id, actor)
              log.debug(s"RE-SEND: $actor ! Persist($key, Some($value), $id)")
              actor ! Persist(key, Some(value), id)
            } else {
              // Actor Persistence should fail after 9 retries
              log.debug(s"FAIL-INSERT-PERSIST: $actor ! Kill")
              actor ! Kill
              log.debug(s"FAIL-INSERT-PERSIST: ${persistenceMessageIdToSenderMap(id)} ! OperationFailed($id)")
              persistenceMessageIdToSenderMap(id) ! OperationFailed(id)
            }
          }
        } else {
          // Persistence already confirmed. Cancel resend.
          log.debug(s"CANCEL: Persist($key, Some($value), $id)")
          persistenceCancellables(id).cancel()
          persistenceCancellables -= id
        }
      }
      persistenceCancellables += (id -> cancellablePersistence)

      // Schedule to send OperationAck message
      trackAckMessageIdToSender(id)
      val ackCancellable: Cancellable = context.system.scheduler.schedule(100 millisecond, 100 milliseconds) {
        if (ackMessageIdToQuantityMap.get(id).isDefined) {
          if (ackMessageIdToQuantityMap(id) < 10) {
            if (replicateMessageIdToReplicatorsAndQuantityMap.get(id).isDefined
                || persistenceMessageIdToPersistencerMap.get(id).isDefined) {
              trackAckMessageIdToSender(id)
            } else {
              toBeAckedMessageIdToSenderMap(id) ! OperationAck(id)
              // Operation already confirmed. Cancel
              log.debug(s"CANCEL: OperationAck($id)")
              ackCancellables(id).cancel()
              ackCancellables -= id
            }
          } else {
            // Operation should fail after 9 retries
            log.debug(s"FAIL-INSERT-ACK: ${toBeAckedMessageIdToSenderMap(id)} ! OperationFailed($id)")
            toBeAckedMessageIdToSenderMap(id) ! OperationFailed(id)
          }
        } else {
          // Operation already confirmed. Cancel
          log.debug(s"CANCEL: OperationAck($id)")
          ackCancellables(id).cancel()
          ackCancellables -= id
        }
      }
      ackCancellables += (id -> ackCancellable)

    case Remove(key, id) =>
      if (kv.contains(key)) {
        kv -= key

        // Keep sender of message for Ack purpose
        trackToBeAckedMessageIdToSender(id, sender())

        // Replicate to secondaries
        replicators.foreach(replicator => {
          trackReplicateMessageIdToReplicator(id, replicator)
          log.debug(s"SEND: $replicator ! Replicate($key, None, $id)")
          replicator ! Replicate(key, None, id)

        })

        // Schedule to resend Replicate messages
        val cancellable: Cancellable = context.system.scheduler.schedule(100 millisecond, 100 milliseconds) {
          if (replicateMessageIdToReplicatorsAndQuantityMap.get(id).isDefined) {
            // Replicate may be not confirmed. Check and resend to each replicator.
            val replicatorsAndQuantityMap = replicateMessageIdToReplicatorsAndQuantityMap(id)
            for (replicator <- replicatorsAndQuantityMap.keySet) {
              val times = replicatorsAndQuantityMap(replicator)
              if (times < 10) {
                trackReplicateMessageIdToReplicator(id, replicator)
                log.debug(s"RE-SEND: $replicator ! Replicate($key, None, $id)")
                replicator ! Replicate(key, None, id)
              } else {
                // Generate OperationFailed message to the original sender
                log.debug(s"FAIL-REMOVE-REPLICATE: ${toBeAckedMessageIdToSenderMap(id)} ! OperationFailed($id)")
                toBeAckedMessageIdToSenderMap(id) ! OperationFailed(id)
              }
            }
          } else {
            // Replicate already confirmed. Cancel resend.
            log.debug(s"CANCEL: Replicate($key, None, $id)")
            replicationCancellables(id).cancel()
            replicationCancellables -= id
          }
        }
        // Keep track of generated cancellable to the Replicate messages
        replicationCancellables += (id -> cancellable)

        // Persist
        trackPersistenceMessageIdToPersistencer(id, persistencer)
        trackPersistenceMessageIdToSender(id, sender())
        log.debug(s"SEND: $persistencer ! Persist($key, None, $id)")
        persistencer ! Persist(key, None, id)
        // Schedule to resend Persist message
        val cancellablePersistence: Cancellable =
          context.system.scheduler
            .schedule(0 millisecond, 100 milliseconds) {
              if (persistenceMessageIdToPersistencerMap.get(id).isDefined) {
                val persistenceToQuantityMap = persistenceMessageIdToPersistencerMap(id)
                for (actor <- persistenceToQuantityMap.keySet) {
                  val times = persistenceToQuantityMap(actor)
                  if (times < 10) {
                    // Persistence not confirmed. Resend Persist
                    trackPersistenceMessageIdToPersistencer(id, actor)
                    log.debug(s"RE-SEND: $actor ! Persist($key, None, $id)")
                    actor ! Persist(key, None, id)
                  } else {
                    // Actor Persistence should fail after 9 retries
                    log.debug(s"$persistencer ! Kill")
                    persistencer ! Kill
                    log.debug(s"FAIL-REMOVE-PERSIST: ${persistenceMessageIdToSenderMap(id)} ! OperationFailed($id)")
                    persistenceMessageIdToSenderMap(id) ! OperationFailed(id)
                  }
                }
              } else {
                // Persistence already confirmed. Cancel resend.
                log.debug(s"CANCEL: Persist($key, None, $id)")
                persistenceCancellables(id).cancel()
                persistenceCancellables -= id
              }
            }
        persistenceCancellables += (id -> cancellablePersistence)

        // Schedule to send OperationAck message
        trackAckMessageIdToSender(id)
        val ackCancellable: Cancellable = context.system.scheduler.schedule(100 millisecond, 100 milliseconds) {
          if (ackMessageIdToQuantityMap.get(id).isDefined) {
            if (ackMessageIdToQuantityMap(id) < 10) {
              if (replicateMessageIdToReplicatorsAndQuantityMap.get(id).isDefined
                  || persistenceMessageIdToPersistencerMap.get(id).isDefined) {
                trackAckMessageIdToSender(id)
              } else {
                toBeAckedMessageIdToSenderMap(id) ! OperationAck(id)
                // Operation already confirmed. Cancel
                log.debug(s"CANCEL: OperationAck($id)")
                ackCancellables(id).cancel()
                ackCancellables -= id
              }
            } else {
              // Operation should fail after 9 retries
              log.debug(s"FAIL-REMOVE-ACK: ${toBeAckedMessageIdToSenderMap(id)} ! OperationFailed($id)")
              toBeAckedMessageIdToSenderMap(id) ! OperationFailed(id)
            }
          } else {
            // Operation already confirmed. Cancel
            log.debug(s"CANCEL: OperationAck($id)")
            ackCancellables(id).cancel()
            ackCancellables -= id
          }
        }
        ackCancellables += (id -> ackCancellable)

      } else {
        log.debug(s"SEND: ${sender()} ! OperationAck($id)3")
        sender() ! OperationAck(id)
      }

    case Get(key, id) =>
      log.debug(s"SEND: ${sender()} ! GetResult($key, ${kv.get(key)}, $id)")
      sender() ! GetResult(key, kv.get(key), id)

    case Replicas(receivedReplicas) =>
      val currentReplicas = secondaries.keySet

      val removedReplicas = currentReplicas.diff(receivedReplicas)
      val newReplicas = receivedReplicas.diff(currentReplicas)

      val replicateId = nextReplicateId()

      // Terminate Replicator for each removed Replica
      removedReplicas.foreach(replica => {
        val replicator = secondaries(replica)

        // Cancel schedules
        for ((id, cancellable) <- replicationCancellables) {
          cancellable.cancel()
          replicationCancellables -= id
        }
        for ((id, cancellable) <- persistenceCancellables) {
          cancellable.cancel()
          persistenceCancellables -= id
        }

        log.debug(s"SEND: $replica ! PoisonPill")
        replica ! PoisonPill

        log.debug(s"SEND: $replicator ! PoisonPill")
        replicator ! PoisonPill

        secondaries -= replica
        replicators -= replicator
      })

      // Create Replicator for each new Replica
      secondaries ++= newReplicas.drop(1).map { replica =>
        val replicator = context.actorOf(Replicator.props(replica))
        replicators += replicator
        // Forward update event for each known message to the new Replicator
        for ((key, value) <- kv) {
          log.debug(s"SEND: $replicator ! Replicate($key, Some($value), $replicateId)")
          replicator ! Replicate(key, Some(value), replicateId)
        }

        replica -> replicator
      }

    case Replicated(key, id) =>
      if (replicateMessageIdToReplicatorsAndQuantityMap.get(id).isDefined) {
        var replicatorsAndQuantityMap = replicateMessageIdToReplicatorsAndQuantityMap(id)
        replicatorsAndQuantityMap -= sender()
        if (replicatorsAndQuantityMap.isEmpty) {
          replicateMessageIdToReplicatorsAndQuantityMap -= id
        } else {
          replicateMessageIdToReplicatorsAndQuantityMap += (id -> replicatorsAndQuantityMap)
        }
      }

    case Persisted(key, id) =>
      if (persistenceMessageIdToPersistencerMap.get(id).isDefined) {
        persistenceMessageIdToPersistencerMap -= id
        persistenceMessageIdToSenderMap -= id
      }
  }

  /* Behavior for the replica role. */
  val replica: Receive = LoggingReceive {
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)

    case Snapshot(key, valueOption, seq) =>
      if (seq == expectedSnapshotAckId()) {
        valueOption match {
          case Some(value) =>
            // Save locally
            kv += (key -> value)
            // Persist
            trackPersistenceMessageIdToPersistencer(seq, persistencer)
            trackPersistenceMessageIdToSender(seq, sender())
            persistencer ! Persist(key, valueOption, seq)
            // Schedule to resend Persist message
            val cancellable: Cancellable = context.system.scheduler.schedule(100 millisecond, 100 milliseconds) {
              if (persistenceMessageIdToPersistencerMap.get(seq).isDefined) {
                val persistenceToQuantityMap = persistenceMessageIdToPersistencerMap(seq)
                for (actor <- persistenceToQuantityMap.keySet) {
                  val times = persistenceToQuantityMap(actor)
                  if (times < 10) {
                    // Persistence not confirmed. Resend Persist
                    trackPersistenceMessageIdToPersistencer(seq, actor)
                    log.debug(s"RE-SEND: $actor ! Persist($key, $valueOption, $seq)")
                    actor ! Persist(key, valueOption, seq)
                  } else {
                    // Actor Persistence should fail after 9 retries
                    log.debug(s"$actor ! Kill")
                    actor ! Kill
                  }
                }
              } else {
                // Persistence already confirmed. Cancel resend.
                persistenceCancellables(seq).cancel()
                log.debug(s"Canceled replication of message Persist($key, $valueOption, $seq)")
                persistenceCancellables -= seq
              }
            }
            persistenceCancellables += (seq -> cancellable)

            setExpectedSnapshotAckId(seq)
          case None =>
            // Save locally
            kv -= key
            // Persist

            sender() ! SnapshotAck(key, seq)
            setExpectedSnapshotAckId(seq)
        }
      } else if (seq < expectedSnapshotAckId()) {
        sender() ! SnapshotAck(key, seq)
        setExpectedSnapshotAckId(seq)
      }

    case Replicated(key, id) =>
      for ((tempId, replicatorSet) <- replicateMessageIdToReplicatorsAndQuantityMap) {
        if (tempId == id) {
          val newSet = replicatorSet - sender()
          if (newSet.isEmpty) {
            replicateMessageIdToReplicatorsAndQuantityMap -= id

            toBeAckedMessageIdToSenderMap(id) ! OperationAck(id)
            toBeAckedMessageIdToSenderMap -= id
          } else {
            replicateMessageIdToReplicatorsAndQuantityMap += (id -> newSet)
          }
        }
      }

    case Persisted(key, id) =>
      if (persistenceMessageIdToPersistencerMap.get(id).isDefined) {
        persistenceMessageIdToPersistencerMap -= id
        log.debug(s"SEND: ${persistenceMessageIdToSenderMap(id)} ! SnapshotAck(%key, %id)")
        persistenceMessageIdToSenderMap(id) ! SnapshotAck(key, id)
        persistenceMessageIdToSenderMap -= id
      }
  }

  // Connect to the Arbiter
  arbiter ! Join

}
