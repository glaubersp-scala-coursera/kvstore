package kvstore

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Props}
import akka.event.LoggingReceive

import scala.language.postfixOps

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor with ActorLogging {
  import Replicator._
  import context.dispatcher
  import scala.concurrent.duration._

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  var cancellables = Map.empty[Long, Cancellable]

  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]

  var _seqCounter = 0L
  def nextSeq() = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  /* TODO Behavior for the Replicator. */
  def receive: Receive = LoggingReceive {
    case msg @ Replicate(key, valueOption, id) =>
      val seq = nextSeq()

      // Keep the sent Snapshot message until confirmed
      acks += (seq -> ((sender(), msg)))

      // Schedule to re-send Snapshot message to replica on 100ms intervals until confirmed
      val cancellable: Cancellable =
        context.system.scheduler.schedule(0 millisecond, 100 milliseconds) {
          if (acks.get(seq).nonEmpty) {
            // Unconfirmed. Resend!
            replica ! Snapshot(key, valueOption, seq)
          } else {
            // Confirmed. Cancel corresponding schedulled message.
            cancellables(seq).cancel()
            cancellables -= seq
          }
        }
      // Keep track of generated cancellable to the seq Snapshop message
      cancellables += (seq -> cancellable)

    case SnapshotAck(key, seq) =>
      for ((replicateSender, Replicate(repKey, repVal, repId)) <- acks
             .get(seq)) {
        // Remove confirmed Snapshot message
        acks -= seq

        // Confirm Replicate message
        replicateSender ! Replicated(repKey, repId)
      }

  }

}
