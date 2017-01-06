package exercises.messagepassing.abc;

import akka.actor.ActorRef;

import java.io.Serializable;

/**
 * Created by rrjan on 1/2/2017.
 */ // -- MESSAGES --------------------------------------------------
class StartTransferMessage implements Serializable
{
	private final ActorRef bank;
	private final ActorRef from;
	private final ActorRef to;

	public StartTransferMessage(ActorRef bank, ActorRef from, ActorRef to){
		this.bank = bank;
		this.from = from;
		this.to = to;
	}

	public ActorRef getBank(){return bank;}
	public ActorRef getFrom(){return from;}
	public ActorRef getTo(){return to;}
}
