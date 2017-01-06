package exercises.messagepassing.helloworld;

import akka.actor.UntypedActor;

class MyActor extends UntypedActor
{
    // can have (local) state
    private int count = 0;

    public void onReceive(Object o) throws Exception { // reacting to message:
        if (o instanceof MyMessage) {
            MyMessage message = (MyMessage) o;
            System.out.println(message.s + " (" + count + ")");
            count++;
        } else{
            unhandled(o);
        }
    }
}
