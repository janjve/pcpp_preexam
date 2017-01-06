package exercises.messagepassing.abc;

import akka.actor.ActorRef;

import java.io.Serializable;

/**
 * Created by rrjan on 1/2/2017.
 */
class TransferMessage implements Serializable
{
	private final int amount;
	private final ActorRef from;
	private final ActorRef to;

	public TransferMessage(int amount, ActorRef from, ActorRef to){
		this.amount = amount;
		this.from = from;
		this.to = to;
	}

	public ActorRef getFrom(){return from;}
	public ActorRef getTo(){return to;}
	public int getAmount(){return amount;}
}
