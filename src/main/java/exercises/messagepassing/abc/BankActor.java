package exercises.messagepassing.abc;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;

/**
 * Created by rrjan on 1/2/2017.
 */
class BankActor extends UntypedActor
{
	public void onReceive(Object o){
		if(o instanceof TransferMessage){
			TransferMessage message = (TransferMessage)o;
			message.getFrom().tell(new DepositMessage(-1 * message.getAmount()), ActorRef.noSender());
			message.getTo().tell(new DepositMessage(message.getAmount()), ActorRef.noSender());
		}
	}
}
