package exercises.messagepassing.abc;

import java.io.Serializable;

/**
 * Created by rrjan on 1/2/2017.
 */
class DepositMessage implements Serializable
{
	private final int amount;

	public DepositMessage(int amount){
		this.amount = amount;
	}

	public int getAmount(){return amount;}
}
