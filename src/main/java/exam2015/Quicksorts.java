package exam2015;

// For PCPP exam January 2015
// sestoft@itu.dk * 2015-01-03 with post-exam updates 2015-01-09

// Several versions of sequential and parallel quicksort:
// A: sequential recursive
// B: sequential using work a deque as a stack

// To do by students:
// C: single-queue multi-threaded with shared lock-based queue
// D: multi-queue multi-threaded with thread-local lock-based queues and stealing
// E: as D but with thread-local lock-free queues and stealing

import exercises.benchmark.Timer;
import net.jcip.annotations.GuardedBy;
import sun.java2d.pipe.SpanShapeRenderer;

import java.lang.reflect.InvocationTargetException;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

public class Quicksorts
{
    final static int size = 30_000_000; // Number of integers to sort

    public static void main(String[] args)
    {
        //sequentialRecursive();
        //singleQueueSingleThread();
        //singleQueueMultiThread(8);
        //    multiQueueMultiThread(8);
        //    multiQueueMultiThreadCL(8);

        // Question 4
        //DequeTest testSuite = new SimpleDequeTest(new SimpleDeque<>(10_000_000), 4, 1_000_000, 0.2);
        //testSuite.run();

        // Question 5
        //benchmarkSQMT();

        // Question 7
        //benchmarkMQMT(() -> new SimpleDeque<>(100_000));

        // Question 9
        //DequeTest testSuite2 = new ChaseLevDequeTest(new ChaseLevDeque<>(10_000_000), 10_000_000, 4, 0.5);
        //testSuite2.run();

        // Question 10
        benchmarkMQMT(() -> new ChaseLevDeque<>(100_000));
    }

    // ----------------------------------------------------------------------
    // Version A: Standard sequential quicksort using recursion

    private static void sequentialRecursive()
    {
        int[] arr = IntArrayUtil.randomIntArray(size);
        qsort(arr, 0, arr.length - 1);
        System.out.println(IntArrayUtil.isSorted(arr));
    }

    private static void benchmarkSQMT(){
        System.out.printf("Input size: %d\n", size);
        System.out.printf("Threads\tRunningtime\n");
        for(int t = 1; t < 9; t++){
            int[] arr = IntArrayUtil.randomIntArray(size);
            Deque<SortTask> queue = new SimpleDeque<SortTask>(100_000);
            queue.push(new SortTask(arr, 0, arr.length - 1));

            Timer timer = new Timer();
            sqmtWorkers(queue, t);
            double runningtime = timer.check();
            System.out.printf("%d\t%15.8f s\n", t, runningtime);
        }
    }

    private static void benchmarkMQMT(Supplier<Deque<SortTask>> dequeSupplier) {
        System.out.printf("Input size: %d\n", size);
        System.out.printf("Threads\tRunningtime\n");
        for(int t = 1; t < 9; t++){
            int[] arr = IntArrayUtil.randomIntArray(size);

            Deque<SortTask>[] queues = new Deque[t];
            for(int i = 0; i < t; i++){
                queues[i] = dequeSupplier.get();
            }

            queues[0].push(new SortTask(arr, 0, arr.length - 1));

            Timer timer = new Timer();
            mqmtWorkers(queues, t);
            double runningtime = timer.check();
            System.out.printf("%d\t%15.8f s\n", t, runningtime);
        }
    }

    // Sort arr[a..b] endpoints inclusive
    private static void qsort(int[] arr, int a, int b)
    {
        if (a < b)
        {
            int i = a, j = b;
            int x = arr[(i + j) / 2];
            do
            {
                while (arr[i] < x) i++;
                while (arr[j] > x) j--;
                if (i <= j)
                {
                    swap(arr, i, j);
                    i++;
                    j--;
                }
            } while (i <= j);
            qsort(arr, a, j);
            qsort(arr, i, b);
        }
    }

    // Swap arr[s] and arr[t]
    private static void swap(int[] arr, int s, int t)
    {
        int tmp = arr[s];
        arr[s] = arr[t];
        arr[t] = tmp;
    }

    // ----------------------------------------------------------------------
    // Version B: Single-queue single-thread setup; sequential quicksort using queue

    private static void singleQueueSingleThread()
    {
        SimpleDeque<SortTask> queue = new SimpleDeque<SortTask>(100000);
        int[] arr = IntArrayUtil.randomIntArray(size);
        queue.push(new SortTask(arr, 0, arr.length - 1));
        sqstWorker(queue);
        System.out.println(IntArrayUtil.isSorted(arr));
    }

    private static void sqstWorker(Deque<SortTask> queue)
    {
        SortTask task;
        while (null != (task = queue.pop()))
        {
            final int[] arr = task.arr;
            final int a = task.a, b = task.b;
            if (a < b)
            {
                int i = a, j = b;
                int x = arr[(i + j) / 2];
                do
                {
                    while (arr[i] < x) i++;
                    while (arr[j] > x) j--;
                    if (i <= j)
                    {
                        swap(arr, i, j);
                        i++;
                        j--;
                    }
                } while (i <= j);
                queue.push(new SortTask(arr, a, j));
                queue.push(new SortTask(arr, i, b));
            }
        }
    }

    // ----------------------------------------------------------------------
    // Version C: Single-queue multi-thread setup

    private static void singleQueueMultiThread(final int threadCount)
    {
        int[] arr = IntArrayUtil.randomIntArray(size);
        // To do: ... create queue, then call sqmtWorkers(queue, threadCount)
        Deque<SortTask> queue = new SimpleDeque<SortTask>(100_000);
        queue.push(new SortTask(arr, 0, arr.length - 1));

        System.out.print("Before: ");
        IntArrayUtil.printout(arr, 20);

        sqmtWorkers(queue, threadCount);

        System.out.print("After:  ");
        IntArrayUtil.printout(arr, 20);

        System.out.println(IntArrayUtil.isSorted(arr));
    }

    private static void sqmtWorkers(Deque<SortTask> queue, int threadCount)
    {
        LongAdder ongoing = new LongAdder();
        ongoing.increment();
        Thread[] threads = new Thread[threadCount];

        for (int t = 0; t < threadCount; t++)
        {
            final Thread thread = new Thread(() ->
            {
                SortTask task;
                while (null != (task = getTask(queue, ongoing)))
                {
                    final int[] arr = task.arr;
                    final int a = task.a, b = task.b;
                    if (a < b)
                    {
                        int i = a, j = b;
                        int x = arr[(i + j) / 2];
                        do
                        {
                            while (arr[i] < x) i++;
                            while (arr[j] > x) j--;
                            if (i <= j)
                            {
                                swap(arr, i, j);
                                i++;
                                j--;
                            }
                        } while (i <= j);
                        appendTask(queue, ongoing, new SortTask(arr, a, j));
                        appendTask(queue, ongoing, new SortTask(arr, i, b));
                    }
                    ongoing.decrement();
                }
            });
            thread.start();
            threads[t] = thread;
        }

        // Wait for threads to finish.
        try
        {
            for (int t = 0; t < threadCount; t++)
            {
                threads[t].join();
            }
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    private static void appendTask(final Deque<SortTask> queue, LongAdder adder, SortTask task)
    {
        queue.push(task);
        adder.increment();
    }
    // Tries to get a sorting task.  If task queue is empty but some
    // tasks are not yet processed, yield and then try again.

    private static SortTask getTask(final Deque<SortTask> queue, LongAdder ongoing)
    {
        SortTask task;
        while (null == (task = queue.pop()))
        {
            if (ongoing.longValue() > 0)
                Thread.yield();
            else
                return null;
        }
        return task;
    }




    // ----------------------------------------------------------------------
    // Version D: Multi-queue multi-thread setup, thread-local queues

    private static void multiQueueMultiThread(final int threadCount)
    {
        int[] arr = IntArrayUtil.randomIntArray(size);
        Deque<SortTask>[] queues = new Deque[threadCount];
        for(int t = 0; t < threadCount; t++){
            SimpleDeque<SortTask> queue = new SimpleDeque<>(100_000);
            queues[t] = queue;
        }

        // Assume single queue will have initial task.
        queues[0].push(new SortTask(arr, 0, arr.length - 1));
        mqmtWorkers(queues, threadCount);

        System.out.println(IntArrayUtil.isSorted(arr));
    }

    // Version E: Multi-queue multi-thread setup, thread-local queues

    private static void multiQueueMultiThreadCL(final int threadCount)
    {
        int[] arr = IntArrayUtil.randomIntArray(size);
        Deque<SortTask>[] queues = new Deque[threadCount];
        for(int t = 0; t < threadCount; t++){
            ChaseLevDeque<SortTask> queue = new ChaseLevDeque<>(100_000);
            queues[t] = queue;
        }

        // Assume single queue will have initial task.
        queues[0].push(new SortTask(arr, 0, arr.length - 1));
        mqmtWorkers(queues, threadCount);

        System.out.println(IntArrayUtil.isSorted(arr));
    }

    private static void mqmtWorkers(Deque<SortTask>[] queues, int threadCount)
    {
        LongAdder ongoing = new LongAdder();
        ongoing.increment();
        Thread[] threads = new Thread[threadCount];

        for (int t = 0; t < threadCount; t++)
        {
            final int threadNo = t;
            final Thread thread = new Thread(() ->
            {
                SortTask task;
                while (null != (task = getTask(threadNo, queues, ongoing)))
                {
                    final int[] arr = task.arr;
                    final int a = task.a, b = task.b;
                    if (a < b)
                    {
                        int i = a, j = b;
                        int x = arr[(i + j) / 2];
                        do
                        {
                            while (arr[i] < x) i++;
                            while (arr[j] > x) j--;
                            if (i <= j)
                            {
                                swap(arr, i, j);
                                i++;
                                j--;
                            }
                        } while (i <= j);
                        appendTask(queues[threadNo], ongoing, new SortTask(arr, a, j));
                        appendTask(queues[threadNo], ongoing, new SortTask(arr, i, b));
                    }
                    ongoing.decrement();
                }
            });
            thread.start();
            threads[t] = thread;
        }

        // Wait for threads to finish.
        try
        {
            for (int t = 0; t < threadCount; t++)
            {
                threads[t].join();
            }
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    // Tries to get a sorting task.  If task queue is empty, repeatedly
    // try to steal, cyclically, from other threads and if that fails,
    // yield and then try again, while some sort tasks are not processed.

    private static SortTask getTask(final int myNumber, final Deque<SortTask>[] queues,
                                    LongAdder ongoing)
    {
        final int threadCount = queues.length;
        SortTask task = queues[myNumber].pop();
        if (null != task)
            return task;
        else
        {
            do
            {
                for(int t = 0; t < queues.length; t++){
                    if(t == myNumber) continue;

                    task = queues[t].steal();
                    if(null != task)
                        return task;
                }

                Thread.yield();
            } while (ongoing.longValue() > 0);
            return null;
        }
    }
}

// ----------------------------------------------------------------------
// SortTask class, Deque<T> interface, SimpleDeque<T>

// Represents the task of sorting arr[a..b]
class SortTask
{
    public final int[] arr;
    public final int a, b;

    public SortTask(int[] arr, int a, int b)
    {
        this.arr = arr;
        this.a = a;
        this.b = b;
    }
}

interface Deque<T>
{
    void push(T item);    // at bottom

    T pop();              // from bottom

    T steal();            // from top
}

class SimpleDeque<T> implements Deque<T>
{
    // The queue's items are in items[top%S...(bottom-1)%S], where S ==
    // items.length; items[bottom%S] is where the next push will happen;
    // items[(bottom-1)%S] is where the next pop will happen;
    // items[top%S] is where the next steal will happen; the queue is
    // empty if top == bottom, non-empty if top < bottom, and full if
    // bottom - top == items.length.  The top field can only increase.
    @GuardedBy("lock")
    private long bottom = 0;
    @GuardedBy("lock")
    private long top = 0;
    @GuardedBy("lock")
    private final T[] items;
    private final Object lock = new Object();


    public SimpleDeque(int size)
    {
        this.items = makeArray(size);
    }

    @SuppressWarnings("unchecked")
    private static <T> T[] makeArray(int size)
    {
        // Java's @$#@?!! type system requires this unsafe cast
        return (T[]) new Object[size];
    }

    private static int index(long i, int n)
    {
        return (int) (i % (long) n);
    }

    public void push(T item)
    { // at bottom
        synchronized (lock)
        {
            final long size = bottom - top;
            if (size == items.length)
                throw new RuntimeException("queue overflow");
            items[index(bottom++, items.length)] = item;
        }
    }

    public T pop()
    { // from bottom
        synchronized (lock)
        {
            final long afterSize = bottom - 1 - top;
            if (afterSize < 0)
                return null;
            else
                return items[index(--bottom, items.length)];
        }
    }

    public T steal()
    { // from top
        synchronized (lock)
        {
            final long size = bottom - top;
            if (size <= 0)
                return null;
            else
                return items[index(top++, items.length)];
        }
    }
}

// ----------------------------------------------------------------------

// A lock-free queue simplified from Chase and Lev: Dynamic circular
// work-stealing queue, SPAA 2005.  We simplify it by not reallocating
// the array; hence this queue may overflow.  This is close in spirit
// to the original ABP work-stealing queue (Arora, Blumofe, Plaxton:
// Thread scheduling for multiprogrammed multiprocessors, 2000,
// section 3) but in that paper an "age" tag needs to be added to the
// top pointer to avoid the ABA problem (see ABP section 3.3).  This
// is not necessary in the Chase-Lev dequeue design, where the top
// index never assumes the same value twice.

class ChaseLevDeque<T> implements Deque<T> {
    private volatile long bottom = 0;
    private final AtomicLong top = new AtomicLong();
    private final T[] items;

    public ChaseLevDeque(int size) {
        this.items = makeArray(size);
    }

    @SuppressWarnings("unchecked")
    private static <T> T[] makeArray(int size) {
        // Java's @$#@?!! type system requires this unsafe cast
        return (T[])new Object[size];
    }

    private static int index(long i, int n) {
        return (int)(i % (long)n);
    }

    public void push(T item) { // at bottom
        final long b = bottom;
        final long t = top.longValue();
        final long size = b - t;
        if (size == items.length)
            throw new RuntimeException("queue overflow");
        items[index(b, items.length)] = item;
        bottom = b+1;
    }

    public T pop() { // from bottom
        final long b = bottom - 1;
        bottom = b;
        final long t = top.longValue();
        final long afterSize = b - t;
        if (afterSize < 0) { // empty before call
            bottom = t;
            return null;
        } else {
            T result = items[index(b, items.length)];
            if (afterSize > 0) // non-empty after call
                return result;
            else {		// became empty, update both top and bottom
                if (!top.compareAndSet(t, t+1)) // somebody stole result
                    result = null;
                bottom = t+1;
                return result;
            }
        }
    }

    public T steal() { // from top
        final long t = top.longValue();
        final long b = bottom;
        final long size = b - t;
        if (size <= 0)
            return null;
        else {
            T result = items[index(t, items.length)];
            if (top.compareAndSet(t, t+1))
                return result;
            else
                return null;
        }
    }
}


abstract class DequeTest {
    protected final Deque<Integer> deque;
    protected final int nTrials;
    protected final AtomicLong popStealSum = new AtomicLong(0);
    protected final AtomicLong pushSum = new AtomicLong(0);

    protected DequeTest(Deque<Integer> queue, int n){
        deque = queue;
        nTrials = n;
    }

    protected abstract void run();

    protected void testDequeSequential(){
        System.out.print("Starting testDequeSequential");
        Deque<Integer> queue = this.deque;
        // Push-pop
        queue.push(1);
        assert queue.pop() == 1;

        // take from empty
        assert queue.pop() == null;
        assert queue.steal() == null;

        // Top-bottom
        queue.push(1);
        queue.push(2);
        queue.push(3);

        assert queue.steal() == 1;
        assert queue.pop() == 3;
        assert queue.pop() == 2;
        assert queue.steal() == null;
        System.out.println("... passed");
    }
}

// Question 4
// ----------------------------------------------------------------------
class SimpleDequeTest extends DequeTest{
    private CyclicBarrier startBarrier, endBarrier;
    private final int producerConsumerPairs;
    protected final double popStealRatio;

    public SimpleDequeTest (SimpleDeque<Integer> queue, int pcPairs, int n, double psr){
        super(queue, n);
        startBarrier = new CyclicBarrier(pcPairs * 2 + 1);
        endBarrier = new CyclicBarrier(pcPairs * 2 + 1);
        producerConsumerPairs = pcPairs;
        popStealRatio = psr;
    }

    @Override
    public void run(){
        System.out.println("SimpleDeque test: ");
        testDequeSequential();
        ExecutorService pool = Executors.newWorkStealingPool();
        testDequeConcurrent(pool);
        pool.shutdown();
        System.out.println("All passed.");
    }

    private void testDequeConcurrent(ExecutorService pool){
        System.out.print("Starting testSimpleDequeConcurrent");
        for(int t = 0; t < producerConsumerPairs; t++){
            pool.execute(new Producer());
            pool.execute(new Consumer());
        }

        try
        {
            startBarrier.await();
            endBarrier.await();
        } catch (Exception e)
        {
            e.printStackTrace();
        }
        pool.shutdown();

        // Assert: Sums should match and queue be empty.
        assert pushSum.longValue() == popStealSum.longValue();
        assert deque.pop() == null;
        assert deque.steal() == null;
        System.out.println("... passed");
    }


    class Producer implements Runnable{
        public void run() {
            try {
                Random random = new Random();
                int sum = 0;
                startBarrier.await();

                for (int i = nTrials; i > 0; --i) {
                    int item = random.nextInt();
                    deque.push(item);
                    sum += item;
                }
                pushSum.getAndAdd(sum);
                endBarrier.await();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    class Consumer implements Runnable
    {
        public void run()
        {
            try
            {
                Random random = new Random();
                int sum = 0;
                startBarrier.await();

                for (int i = nTrials; i > 0; --i)
                {
                    Integer v;
                    Boolean shouldSteal = random.nextDouble() > popStealRatio;
                    do
                    {
                        v = shouldSteal ? deque.steal() : deque.pop();
                    } while (v == null);
                    sum += v;
                }
                popStealSum.getAndAdd(sum);
                endBarrier.await();
            } catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}

// Question 9
// ----------------------------------------------------------------------
class ChaseLevDequeTest extends DequeTest {
    private CyclicBarrier startBarrier, endBarrier;
    private final int consumerCount;
    private final double stealRatio;
    // Should be between 0 and 1, where 1 is only steal and 0 is only pop.

    public ChaseLevDequeTest(Deque<Integer> queue, int n, int cc, double sr){
        super(queue, n);
        startBarrier = new CyclicBarrier(cc + 2); // main thread, producer and no. of consumers
        endBarrier = new CyclicBarrier(cc + 2);
        consumerCount = cc;
        stealRatio = sr;
    }

    @Override
    protected void run()
    {
        System.out.println("ChaseLevDeque test: ");
        testDequeSequential();
        ExecutorService pool = Executors.newWorkStealingPool();
        testDequeConcurrent(pool);
        pool.shutdown();
        System.out.println("All passed.");
    }

    private void testDequeConcurrent(ExecutorService pool){
        System.out.print("Starting testDequeConcurrent");
        final int steals = (int)(stealRatio * nTrials);
        pool.execute(new Producer(nTrials - steals));
        final int remainder = steals % consumerCount;
        for(int t = 0; t < consumerCount; t++){
            final int n = remainder > t ?
                    (steals / consumerCount) + 1 : (steals / consumerCount);
            pool.execute(new Consumer(n));
        }

        try
        {
            startBarrier.await();
            endBarrier.await();
        } catch (Exception e)
        {
            e.printStackTrace();
        }
        pool.shutdown();

        // Assert: Sums should match and queue be empty.

        System.out.println("Asserting");
        assert pushSum.longValue() == popStealSum.longValue();
        assert deque.pop() == null;
        assert deque.steal() == null;
        System.out.println("... passed");
    }

    class Producer implements Runnable {
        private final int pops;
        public Producer(int n){
            pops = n;
        }
        @Override
        public void run()
        {
            try {
                Random random = new Random();
                long sumIn = 0;
                long sumOut = 0;
                startBarrier.await();
                int i = nTrials, j=pops;
                while(i > 0 || j > 0){
                    if(j > 0 && (i == 0 || random.nextBoolean())){
                        Integer v;
                        v = deque.pop();
                        if(null != v){
                            // We cannot guarantee pop, because all values might be stolen
                            // and we're the only thread enqueueing new values.
                            sumOut += v;
                            j--;
                        }
                    } else if (i > 0)
                    {
                        int item = random.nextInt(100);
                        deque.push(item);
                        sumIn += item;
                        i--;
                    }
                }

                pushSum.getAndAdd(sumIn);
                popStealSum.getAndAdd(sumOut);
                System.out.println("producer");
                endBarrier.await();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    class Consumer implements Runnable {
        private final int nCount;
        public Consumer(int n){
            nCount = n;
        }

        @Override
        public void run()
        {
            try
            {
                int sum = 0;
                startBarrier.await();

                for (int i = nCount; i > 0; --i)
                {
                    Integer v;
                    do
                    {
                        v = deque.steal();
                    } while (v == null);
                    sum += v;
                }
                popStealSum.getAndAdd(sum);
                System.out.println("Consumer");

                endBarrier.await();
            } catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}


// ----------------------------------------------------------------------
class IntArrayUtil
{
    public static int[] randomIntArray(final int n)
    {
        int[] arr = new int[n];
        for (int i = 0; i < n; i++)
            arr[i] = (int) (Math.random() * n * 2);
        return arr;
    }

    public static void printout(final int[] arr, final int n)
    {
        for (int i = 0; i < n; i++)
            System.out.print(arr[i] + " ");
        System.out.println("");
    }

    public static boolean isSorted(final int[] arr)
    {
        for (int i = 1; i < arr.length; i++)
            if (arr[i - 1] > arr[i])
                return false;
        return true;
    }
}