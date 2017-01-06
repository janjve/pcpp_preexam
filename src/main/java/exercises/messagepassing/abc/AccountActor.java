package exercises.messagepassing.abc;

import akka.actor.UntypedActor;

/**
 * Created by rrjan on 1/2/2017.
 */ // -- ACTORS --------------------------------------------------
class AccountActor extends UntypedActor
{
	private int balance;

	public AccountActor(int balance){
		this.balance = balance;
	}

	public void onReceive(Object o){
		if(o instanceof DepositMessage){
			DepositMessage message = (DepositMessage)o;
			balance += message.getAmount();
		} else if(o instanceof PrintBalanceMessage){
			System.out.println(balance);
		}
	}
}
