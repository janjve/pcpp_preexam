package exercises.messagepassing.abc;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;

import java.util.Random;

/**
 * Created by rrjan on 1/2/2017.
 */
class ClerkActor extends UntypedActor
{
	private Random random;

	private void ntransfers(int n, ActorRef bank, ActorRef from, ActorRef to){
		if(n == 0) return;
		else {
			int r = random.nextInt(100);
			bank.tell(new TransferMessage(r, from, to), ActorRef.noSender());
			ntransfers(n-1, bank, from, to);
		}
	}

	public void onReceive(Object o){
		if(o instanceof StartTransferMessage){
			StartTransferMessage message = (StartTransferMessage)o;
			random = new Random();
			ntransfers(100, message.getBank(), message.getFrom(), message.getTo());
		}
	}
}
