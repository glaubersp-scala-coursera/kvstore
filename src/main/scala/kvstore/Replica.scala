package kvstore

import akka.actor.{Actor, ActorRef, Cancellable, PoisonPill, Props}
import akka.event.LoggingReceive
import kvstore.Arbiter._

import scala.concurrent.duration.FiniteDuration

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
  case class GetResult(key: String, valueOption: Option[String], id: Long)
      extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props =
    Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import context.dispatcher
  import scala.concurrent.duration._

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  // Keep track of Replicate Messages
  var replicateMessageIdToReplicatorsMap = Map.empty[Long, Set[ActorRef]]
  def trackReplicateMessageIdToReplicator(replicateId: Long,
                                          replicator: ActorRef): Unit = {
    var replicatorSet = Set.empty[ActorRef]
    if (replicateMessageIdToReplicatorsMap.get(replicateId).isDefined) {
      replicatorSet = replicateMessageIdToReplicatorsMap(replicateId)
    }
    replicatorSet += replicator
    replicateMessageIdToReplicatorsMap += (replicateId -> replicatorSet)
  }

  var messageIdToSenderMap = Map.empty[Long, ActorRef]
  def trackMessageIdToSender(msgId: Long, sender: ActorRef): Unit = {
    messageIdToSenderMap += (msgId -> sender)
  }

  var replicationCancellables = Map.empty[Long, Cancellable]

  // The Persistence actor
  val persistence = context.actorOf(persistenceProps)

  var _replicateIdCounter = 0L
  def nextReplicateId(): Long = {
    val ret = _replicateIdCounter
    _replicateIdCounter += 1
    ret
  }

  var _snapshotAckCounter = 0L

  def expectedSnapshotAckId(): Long = {
    _snapshotAckCounter
  }
  def setExpectedSnapshotAckId(lastAckId: Long): Unit = {
    _snapshotAckCounter = _snapshotAckCounter max (lastAckId + 1)
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = LoggingReceive {

    case Insert(key, value, id) =>
      kv += (key -> value)
      // Persist change

      // Keep sender of message for Ack purpouse
      trackMessageIdToSender(id, sender())

      // Replicate to secondaries
      replicators.foreach(replicator => {
        trackReplicateMessageIdToReplicator(id, replicator)
        replicator ! Replicate(key, Some(value), id)
      })
      // Schedule to send OperationAck message
      val cancellable: Cancellable = context.system.scheduler
        .schedule(FiniteDuration(0, "ms"), FiniteDuration(100, "ms")) {
          if (replicateMessageIdToReplicatorsMap.get(id).isEmpty) {
            // All Replicate confirmed. Send OperationAck
            messageIdToSenderMap(id) ! OperationAck(id)
            replicationCancellables(id).cancel()
            replicationCancellables -= id
          }
        }
      // Keep track of generated cancellable to the Replicate messages
      replicationCancellables += (id -> cancellable)

    case Remove(key, id) =>
      if (kv.contains(key)) {
        kv -= key

        // Keep sender of message for Ack purpouse
        trackMessageIdToSender(id, sender())

        // Replicate to secondaries
        replicators.foreach(replicator => {
          trackReplicateMessageIdToReplicator(id, replicator)
          replicator ! Replicate(key, None, id)
        })

        // Persist change

        // Schedule to send OperationAck message
        val cancellable: Cancellable = context.system.scheduler
          .schedule(FiniteDuration(0, "ms"), FiniteDuration(100, "ms")) {
            if (replicateMessageIdToReplicatorsMap.get(id).isEmpty) {
              // All Replicate confirmed. Send OperationAck
              messageIdToSenderMap(id) ! OperationAck(id)
              replicationCancellables(id).cancel()
              replicationCancellables -= id
            }
          }
        // Keep track of generated cancellable to the Replicate messages
        replicationCancellables += (id -> cancellable)
      } else {
        sender() ! OperationAck(id)
      }

    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)

    case Replicas(receivedReplicas) =>
      val currentReplicas = secondaries.keySet

      val removedReplicas = currentReplicas.diff(receivedReplicas)
      val newReplicas = receivedReplicas.diff(currentReplicas)

      val replicateId = nextReplicateId()

      // Terminate Replicator for each removed Replica
      removedReplicas.foreach(replica => {
        val replicator = secondaries(replica)
        replica ! PoisonPill
        replicator ! PoisonPill
        secondaries -= replica
        replicators -= replicator
      })

      // Create Replicator for each new Replica
      secondaries ++= newReplicas.map { replica =>
        val replicator = context.actorOf(Replicator.props(replica))
        replicators += replicator
        // Forward update event for each known message to the new Replicator
        for ((key, value) <- kv) {
          replicator ! Replicate(key, Some(value), replicateId)
        }

        replica -> replicator
      }

    case Replicated(key, id) =>
      for ((tempId, replicatorSet) <- replicateMessageIdToReplicatorsMap) {
        if (tempId == id) {
          val newSet = replicatorSet - sender()
          if (newSet.isEmpty) {
            replicateMessageIdToReplicatorsMap -= id

            messageIdToSenderMap(id) ! OperationAck(id)
            messageIdToSenderMap -= id
          } else {
            replicateMessageIdToReplicatorsMap += (id -> replicatorSet)
          }
        }
      }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = LoggingReceive {
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)

    case Snapshot(key, valueOption, seq) =>
      if (seq == expectedSnapshotAckId()) {
        valueOption match {
          case Some(value) =>
            kv += (key -> value)
            sender() ! SnapshotAck(key, seq)
            setExpectedSnapshotAckId(seq)
          case None =>
            kv -= key
            sender() ! SnapshotAck(key, seq)
            setExpectedSnapshotAckId(seq)
        }
      } else if (seq < expectedSnapshotAckId()) {
        sender() ! SnapshotAck(key, seq)
        setExpectedSnapshotAckId(seq)
      }

    case Replicated(key, id) =>
      for ((tempId, replicatorSet) <- replicateMessageIdToReplicatorsMap) {
        if (tempId == id) {
          val newSet = replicatorSet - sender()
          if (newSet.isEmpty) {
            replicateMessageIdToReplicatorsMap -= id

            messageIdToSenderMap(id) ! OperationAck(id)
            messageIdToSenderMap -= id
          } else {
            replicateMessageIdToReplicatorsMap += (id -> replicatorSet)
          }
        }
      }
  }

  // Connect to the Arbiter
  arbiter ! Join

  // Create the persistence actor
}
