package exercises.multiverse;

import org.multiverse.api.references.TxnBoolean;

import static org.multiverse.api.StmUtils.atomic;
import static org.multiverse.api.StmUtils.retry;

class Philosopher implements Runnable
{
    private final TxnBoolean[] forks;
    private final int place;

    public Philosopher(TxnBoolean[] forks, int place)
    {
        this.forks = forks;
        this.place = place;
    }

    public void run()
    {
        while (true)
        {
            System.out.println();
            // Take the two forks to the left and the right
            final int left = place, right = (place + 1) % forks.length;
            atomic(() ->
            {
                // System.out.printf("[%d]", place);
                if (!forks[left].get() && !forks[right].get())
                {
                    forks[left].set(true);
                    forks[right].set(true);
                } else
                    retry();
            });
            System.out.printf("%d ", place);  // Eat
            atomic(() ->
            {
                forks[left].set(false);
                forks[right].set(false);
            });
            try
            {
                Thread.sleep(10);
            }         // Think
            catch (InterruptedException exn)
            {
            }
        }
    }
}
