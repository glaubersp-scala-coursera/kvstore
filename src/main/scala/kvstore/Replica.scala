package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, PoisonPill, Props}
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
  // Keep track of Replicate Messages and Replicators
  var replicateMessageIdToReplicatorsMap = Map.empty[Long, Set[ActorRef]]

  def trackReplicateMessageIdToReplicator(replicateId: Long, replicator: ActorRef): Unit = {
    var replicatorSet = Set.empty[ActorRef]
    if (replicateMessageIdToReplicatorsMap.get(replicateId).isDefined) {
      replicatorSet = replicateMessageIdToReplicatorsMap(replicateId)
    }
    replicatorSet += replicator
    replicateMessageIdToReplicatorsMap += (replicateId -> replicatorSet)
  }

  // Keep track of Replicate messages and senders
  var replicateMessageIdToSenderMap = Map.empty[Long, ActorRef]

  def trackReplicateMessageIdToSender(msgId: Long, sender: ActorRef): Unit = {
    replicateMessageIdToSenderMap += (msgId -> sender)
  }

  var replicationCancellables = Map.empty[Long, Cancellable]
  // ================ END OF REPLICATION ===================

  // ================ PERSISTENCE ===================
  var persistenceCancellables = Map.empty[Long, Cancellable]

  // The Persistence actor
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 second) {
      case _: Exception => Restart
    }
  val persistencer = context.actorOf(persistenceProps)

  // Keep track of Persistence Messages and Persistencer
  var persistenceMessageIdToPersistencerMap = Map.empty[Long, ActorRef]

  def trackPersistenceMessageIdToPersistencer(msgId: Long, persistencer: ActorRef): Unit = {
    persistenceMessageIdToPersistencerMap += (msgId -> persistencer)
  }

  // Keep track of Persistence messages and senders
  var persistenceMessageIdToSenderMap = Map.empty[Long, ActorRef]

  def trackPersistenceMessageIdToSender(msgId: Long, sender: ActorRef): Unit = {
    persistenceMessageIdToSenderMap += (msgId -> sender)
  }

  // ================ END OF PERSISTENCE ===================

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
      trackReplicateMessageIdToSender(id, sender())

      // Replicate to secondaries
      replicators.foreach(replicator => {
        trackReplicateMessageIdToReplicator(id, replicator)
        replicator ! Replicate(key, Some(value), id)
      })
      // Schedule to send OperationAck message
      val cancellable: Cancellable = context.system.scheduler.schedule(0 millisecond, 100 milliseconds) {
        if (replicateMessageIdToReplicatorsMap.get(id).isEmpty) {
          // All Replicate confirmed. Send OperationAck
          replicateMessageIdToSenderMap(id) ! OperationAck(id)
          replicationCancellables(id).cancel()
          replicationCancellables -= id
        }
      }
      // Keep track of generated cancellable to the Replicate messages
      replicationCancellables += (id -> cancellable)

    case Remove(key, id) =>
      if (kv.contains(key)) {
        kv -= key

        // Keep sender of message for Ack purpose
        trackReplicateMessageIdToSender(id, sender())

        // Replicate to secondaries
        replicators.foreach(replicator => {
          trackReplicateMessageIdToReplicator(id, replicator)
          replicator ! Replicate(key, None, id)
        })

        // Persist change

        // Schedule to send OperationAck message
        val cancellable: Cancellable = context.system.scheduler.schedule(0 millisecond, 100 milliseconds) {
          if (replicateMessageIdToReplicatorsMap.get(id).isEmpty) {
            // All Replicate confirmed. Send OperationAck
            replicateMessageIdToSenderMap(id) ! OperationAck(id)
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

            replicateMessageIdToSenderMap(id) ! OperationAck(id)
            replicateMessageIdToSenderMap -= id
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
            // Save locally
            kv += (key -> value)
            // Persist
            trackPersistenceMessageIdToPersistencer(seq, persistencer)
            trackPersistenceMessageIdToSender(seq, sender())
            persistencer ! Persist(key, valueOption, seq)
            // Schedule to resend Persist message
            val cancellable: Cancellable = context.system.scheduler.schedule(0 millisecond, 100 milliseconds) {
              if (persistenceMessageIdToPersistencerMap.get(seq).isDefined) {
                // Persistence not confirmed. Resend Persist
                persistenceMessageIdToPersistencerMap(seq) ! Persist(key, valueOption, seq)
              } else {
                // Persistence already confirmed. Cancel resend.
                persistenceCancellables(seq).cancel()
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
      for ((tempId, replicatorSet) <- replicateMessageIdToReplicatorsMap) {
        if (tempId == id) {
          val newSet = replicatorSet - sender()
          if (newSet.isEmpty) {
            replicateMessageIdToReplicatorsMap -= id

            replicateMessageIdToSenderMap(id) ! OperationAck(id)
            replicateMessageIdToSenderMap -= id
          } else {
            replicateMessageIdToReplicatorsMap += (id -> replicatorSet)
          }
        }
      }

    case Persisted(key, id) =>
      for ((tempId, persistencer) <- persistenceMessageIdToPersistencerMap) {
        if (tempId == id) {

          persistenceMessageIdToPersistencerMap -= id

          persistenceMessageIdToSenderMap(id) ! SnapshotAck(key, id)
          persistenceMessageIdToSenderMap -= id
        }
      }
  }

  // Connect to the Arbiter
  arbiter ! Join

}
