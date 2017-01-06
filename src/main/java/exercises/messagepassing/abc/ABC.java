package exercises.messagepassing.abc;

import java.io.*; import akka.actor.*;

// -- MAIN --------------------------------------------------
public class ABC { // Demo showing how things work:
 	public static void main(String[] args) {
		 final ActorSystem system = ActorSystem.create("ABCSystem");

		 final ActorRef c1 = system.actorOf(Props.create(ClerkActor.class), "c1");
		 final ActorRef c2 = system.actorOf(Props.create(ClerkActor.class), "c2");
		 final ActorRef b1 = system.actorOf(Props.create(BankActor.class), "b1");
		 final ActorRef b2 = system.actorOf(Props.create(BankActor.class), "b2");
		 final ActorRef a1 = system.actorOf(Props.create(AccountActor.class, 0), "a1");
		 final ActorRef a2 = system.actorOf(Props.create(AccountActor.class, 0), "a2");

		 c1.tell(new StartTransferMessage(b1, a1, a2), ActorRef.noSender());
		 c2.tell(new StartTransferMessage(b2, a2, a1), ActorRef.noSender());

		 try {
			 System.out.println("Press return to inspect...");
			 System.in.read();

			 a1.tell(new PrintBalanceMessage(), ActorRef.noSender());
			 a2.tell(new PrintBalanceMessage(), ActorRef.noSender());

			 //System.out.println("Press return to terminate...");
			 //System.in.read();
		 } catch(IOException e) {
		 	e.printStackTrace();
		 } finally {
		 	system.terminate();
		 }
 	}
} 