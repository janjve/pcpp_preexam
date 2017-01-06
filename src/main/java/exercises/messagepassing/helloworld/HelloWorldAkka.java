package exercises.messagepassing.helloworld;

import akka.actor.*;
import java.io.IOException;

public class HelloWorldAkka
{
    public static void main(String[] args){
        final ActorSystem system = ActorSystem.create("HelloWorldSystem");
        final ActorRef myactor = system.actorOf(Props.create(MyActor.class), "myactor");

        try {
            myactor.tell(new MyMessage("hello"), ActorRef.noSender());
            myactor.tell(new MyMessage("world"), ActorRef.noSender());
            System.out.println("Press return to terminate...");
            System.in.read();
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            system.terminate();
        }
    }
}


