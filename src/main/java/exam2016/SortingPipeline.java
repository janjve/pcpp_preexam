package exam2016;// Pipelined sorting using P>=1 stages, each maintaining an internal
// collection of size S>=1.  Stage 1 contains the largest items, stage
// 2 the second largest, ..., stage P the smallest ones.  In each
// stage, the internal collection of items is organized as a minheap.
// When a stage receives an item x and its collection is not full, it
// inserts it in the heap.  If the collection is full and x is less
// than or equal to the collections's least item, it forwards the item
// to the next stage; otherwise forwards the collection's least item
// and inserts x into the collection instead.

// When there are itemCount items and stageCount stages, each stage
// must be able to hold at least ceil(itemCount/stageCount) items,
// which equals (itemCount-1)/stageCount+1.

// sestoft@itu.dk * 2016-01-10

import jdk.nashorn.internal.ir.Block;
import org.multiverse.api.references.TxnDouble;
import org.multiverse.api.references.TxnInteger;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntToDoubleFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.multiverse.api.StmUtils.*;

public class SortingPipeline {
    private static boolean PRINT = false;
    public static void print(String s){
        if(PRINT)
            System.out.println(s);
    }

    public static void main(String[] args) {
        SystemInfo();
        final int count = 100_000, P = 4;
        final double[] arr = DoubleArray.randomPermutation(count);

        //Supplier<BlockingDoubleQueue> queueSupplier = () -> new WrappedArrayDoubleQueue();
        //Supplier<BlockingDoubleQueue> queueSupplier = () -> new BlockingNDoubleQueue();
        //Supplier<BlockingDoubleQueue> queueSupplier = () -> new UnboundedDoubleQueue();
        //Supplier<BlockingDoubleQueue> queueSupplier = () -> new NolockNDoubleQueue();
        //Supplier<BlockingDoubleQueue> queueSupplier = () -> new MSUnboundedDoubleQueue();
        Supplier<BlockingDoubleQueue> queueSupplier = () -> new StmBlockingNDoubleQueue();


        BlockingDoubleQueue[] queues = Stream
                .generate(queueSupplier)
                .limit(P+1)
                .toArray(BlockingDoubleQueue[]::new);

        //sortPipeline(arr, P, queues);

        // Benchmark
        /*Mark7("sortPipeline - WrappedArrayDoubleQueue", i -> {
            final double[] arr1 = DoubleArray.randomPermutation(count);
            sortPipeline(arr1, P, queues);
            return arr1[0];
        });*/

        /*Mark7("sortPipeline - BlockingNDoubleQueue", i -> {
            final double[] arr1 = DoubleArray.randomPermutation(count);
            sortPipeline(arr1, P, queues);
            return arr1[0];
        });*/

        /*Mark7("sortPipeline - UnboundedDoubleQueue", i -> {
            final double[] arr1 = DoubleArray.randomPermutation(count);
            sortPipeline(arr1, P, queues);
            return arr1[0];
        });*/

        /*Mark7("sortPipeline - NolockNDoubleQueue", i -> {
            final double[] arr1 = DoubleArray.randomPermutation(count);
            sortPipeline(arr1, P, queues);
            return arr1[0];
        });*/

        /*Mark7("sortPipeline - MSUnboundedDoubleQueue", i -> {
            final double[] arr1 = DoubleArray.randomPermutation(count);
            sortPipeline(arr1, P, queues);
            return arr1[0];
        });*/

        Mark7("sortPipeline - StmBlockingNDoubleQueue", i -> {
            final double[] arr1 = DoubleArray.randomPermutation(count);
            sortPipeline(arr1, P, queues);
            return arr1[0];
        });
    }

    private static void sortPipeline(double[] arr, int P, BlockingDoubleQueue[] queues) {
        final int S = 25_000;
        Thread[] threads = new Thread[P+2];
        print("Initializing sortPipeline...");

        threads[0] = new Thread(new DoubleGenerator(arr, P*S, queues[0]));
        threads[P+1] = new Thread(new SortedChecker(arr.length, queues[queues.length-1]));

        for(int i = 1; i <= P;i++){
            Thread thread = new Thread(new SortingStage(S,arr.length+(P-i)*S,queues[i-1],queues[i]));
            threads[i] = thread;
        }

        print("Running...");
        for(int t = 0; t < threads.length; t++){
            threads[t].start();
        }
        try {
            for(int t = 0; t < threads.length; t++){
                threads[t].join();
            }
        } catch (InterruptedException exn) { }

        print("Done.");
    }

    static class SortingStage implements Runnable {
        private int itemCount;
        private final BlockingDoubleQueue output;
        private final BlockingDoubleQueue input;
        private final double[] heap;  // Internal collection
        private int heapSize;

        public SortingStage(int size, int itemCount, BlockingDoubleQueue input, BlockingDoubleQueue output){
            this.itemCount = itemCount;
            this.input = input;
            this.output = output;
            this.heap = new double[size];
            this.heapSize = 0;
        }

        public void run() {
            while (itemCount > 0) {
                double x = input.take();
                if (heapSize < heap.length) { // heap not full, put x into it
                    heap[heapSize++] = x;
                    DoubleArray.minheapSiftup(heap, heapSize-1, heapSize-1);
                } else if (x <= heap[0]) { // x is small, forward
                    output.put(x);
                    itemCount--;
                } else { // forward least, replace with x
                    double least = heap[0];
                    heap[0] = x;
                    DoubleArray.minheapSiftdown(heap, 0, heapSize-1);
                    output.put(least);
                    itemCount--;
                }
            }
        }
    }

    static class DoubleGenerator implements Runnable {
        private final BlockingDoubleQueue output;
        private final double[] arr;  // The numbers to feed to output
        private final int infinites;

        public DoubleGenerator(double[] arr, int infinites, BlockingDoubleQueue output) {
            this.arr = arr;
            this.output = output;
            this.infinites = infinites;
        }

        public void run() {
            for (int i=0; i<arr.length; i++)  // The numbers to sort
                output.put(arr[i]);
            for (int i=0; i<infinites; i++)   // Infinite numbers for wash-out
                output.put(Double.POSITIVE_INFINITY);
        }
    }

    static class SortedChecker implements Runnable {
        // If DEBUG is true, print the first 100 numbers received
        private final static boolean DEBUG = false;
        private final BlockingDoubleQueue input;
        private final int itemCount; // the number of items to check

        public SortedChecker(int itemCount, BlockingDoubleQueue input) {
            this.itemCount = itemCount;
            this.input = input;
        }

        public void run() {
            int consumed = 0;
            double last = Double.NEGATIVE_INFINITY;
            while (consumed++ < itemCount) {
                double p = input.take();
                if (DEBUG && consumed <= 100)
                    System.out.print(p + " ");
                if (p <= last)
                    System.out.printf("Elements out of order: %g before %g%n", last, p);
                last = p;
            }
            if (DEBUG)
                System.out.println();
        }
    }

    // --- Benchmarking infrastructure ---

    // NB: Modified to show milliseconds instead of nanoseconds

    public static double Mark7(String msg, IntToDoubleFunction f) {
        int n = 10, count = 1, totalCount = 0;
        double dummy = 0.0, runningTime = 0.0, st = 0.0, sst = 0.0;
        do {
            count *= 2;
            st = sst = 0.0;
            for (int j=0; j<n; j++) {
                Timer t = new Timer();
                for (int i=0; i<count; i++)
                    dummy += f.applyAsDouble(i);
                runningTime = t.check();
                double time = runningTime * 1e3 / count;
                st += time;
                sst += time * time;
                totalCount += count;
            }
        } while (runningTime < 0.25 && count < Integer.MAX_VALUE/2);
        double mean = st/n, sdev = Math.sqrt((sst - mean*mean*n)/(n-1));
        System.out.printf("%-25s %15.1f ms %10.2f %10d%n", msg, mean, sdev, count);
        return dummy / totalCount;
    }

    public static void SystemInfo() {
        System.out.printf("# OS:   %s; %s; %s%n",
                System.getProperty("os.name"),
                System.getProperty("os.version"),
                System.getProperty("os.arch"));
        System.out.printf("# JVM:  %s; %s%n",
                System.getProperty("java.vendor"),
                System.getProperty("java.version"));
        // The processor identifier works only on MS Windows:
        System.out.printf("# CPU:  %s; %d \"cores\"%n",
                System.getenv("PROCESSOR_IDENTIFIER"),
                Runtime.getRuntime().availableProcessors());
        java.util.Date now = new java.util.Date();
        System.out.printf("# Date: %s%n",
                new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(now));
    }

    // Crude wall clock timing utility, measuring time in seconds

    static class Timer {
        private long start, spent = 0;
        public Timer() { play(); }
        public double check() { return (System.nanoTime()-start+spent)/1e9; }
        public void pause() { spent += System.nanoTime()-start; }
        public void play() { start = System.nanoTime(); }
    }
}

// ----------------------------------------------------------------------

// Queue interface

interface BlockingDoubleQueue {
    double take();
    void put(double item);
}

// The queue implementations

class WrappedArrayDoubleQueue implements BlockingDoubleQueue{

    private ArrayBlockingQueue<Double> queue = new ArrayBlockingQueue<Double>(50);

    @Override
    public double take()
    {
        try
        {
            return queue.take();
        } catch (InterruptedException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(double item)
    {
        try
        {
            queue.put(item);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}

class BlockingNDoubleQueue implements BlockingDoubleQueue {
    private final Double[] queue = new Double[50];
    private int head = 0;
    private int tail = 0;
    private final Object lock = new Object();

    @Override
    public double take()
    {
        try
        {
            synchronized (lock){
                do{
                    if(head < tail){            // Check if queue is empty.
                        double item = queue[head];
                        head++;                 // Increment head.
                        lock.notify();          // Notify changes.
                        return item;
                    }
                    lock.wait();                // Wait for put to notify.
                } while(true);                  // Blocking.
            }
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        return 0;                               // Impossible.
    }

    @Override
    public void put(double item)
    {
        try
        {
            synchronized (lock)
            {
                do
                {
                    if (!isFull())              // Check if queue is at capacity.
                    {
                        reallocationCheck();    // Reallocate if necessary.
                        queue[tail++] = item;
                        lock.notify();          // Notify changes.
                        return;
                    }
                    lock.wait();                // Wait for take to notify.
                } while (true);                 // Blocking.
            }
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    private boolean isFull(){                   // Checks if queue is full.
        return tail == queue.length -1 && head == 0;
    }

    private void reallocationCheck(){           // Push items towards index 0.
        if(tail == queue.length - 1){
            for(int h = head, i = 0; h < queue.length; h++, i++){
                queue[i] = queue[h];
            }
            tail -= head;                       // Update tail.
            head = 0;                           // Update head.
        }
    }
}

class UnboundedDoubleQueue implements BlockingDoubleQueue {
    private DoubleNode head;
    private DoubleNode tail;
    private final Object lock = new Object();

    public UnboundedDoubleQueue(){
        head = tail = new DoubleNode(0.0, null);
    }

    @Override
    public double take()
    {
        try
        {
            synchronized (lock)
            {
                do
                {
                    if (head.next != null)  // Check if sentinel.
                    {
                        DoubleNode next = head.next;
                        head = next;
                        double item = next.value;
                        lock.notify();      // Notify if other threads
                                            // are waiting at take.
                        return item;
                    }
                    lock.wait();
                } while(true);
            }
        } catch(Exception e){
            e.printStackTrace();
            return 0;
        }
    }

    @Override
    public void put(double item)
    {
        synchronized (lock)                 // Unbounded - we can always insert!
        {
            DoubleNode node = new DoubleNode(item, null);
            tail.next = node;
            tail = node;
            lock.notify();                  // Still have to notify,
                                            // as take() will still block.
        }
    }

    private static class DoubleNode {               // Simple Node.
        DoubleNode next;
        final double value;

        public DoubleNode(double value, DoubleNode next){
            this.value = value;
            this.next = next;
        }
    }
}

class NolockNDoubleQueue implements BlockingDoubleQueue {
    private final double[] queue = new double[50];
    private volatile int head = 0;
    private volatile int tail = 0;

    @Override
    public double take()
    {
        while (tail == head){} // Empty
        double item = queue[head % queue.length];
        head++;
        return item;
    }

    @Override
    public void put(double item)
    {
        while(tail - head == queue.length){} // Full
        queue[tail % queue.length] = item;
        tail++;
    }
}

class MSUnboundedDoubleQueue implements BlockingDoubleQueue{
    private final AtomicReference<DoubleNode> head;
    private final AtomicReference<DoubleNode> tail;

    public MSUnboundedDoubleQueue(){
        DoubleNode sentinel = new DoubleNode(0, null);
        this.tail = new AtomicReference<>(sentinel);
        this.head = new AtomicReference<>(sentinel);
    }

    @Override
    public double take()
    {
        do{
            DoubleNode first = head.get();
            DoubleNode last = tail.get();
            DoubleNode next = first.next.get();

            if(first == head.get())
            if(first == last){
                if(next != null){
                    tail.compareAndSet(last, next); // Correct quiescent state.
                }
                // Blocking, don't return
            } else {
                double item = next.value;
                if(head.compareAndSet(first, next)){
                    return item;
                }
            }
        } while(true);
    }

    @Override
    public void put(double item)
    {
        DoubleNode node = new DoubleNode(item, null);
        DoubleNode last = tail.get();
        DoubleNode next = last.next.get();
        if(last == tail.get())
        if(next == null){
            if(last.next.compareAndSet(next, node)){
                tail.compareAndSet(last, node); // Might succeed. Or not.
                return;
            }
        } else {
            tail.compareAndSet(last, next); // Try correction.
        }
    }

    static class DoubleNode {               // Simple Node.
        final AtomicReference<DoubleNode> next;
        final double value;

        public DoubleNode(double value, DoubleNode next){
            this.value = value;
            this.next = new AtomicReference<>(next);
        }
    }
}

class StmBlockingNDoubleQueue implements BlockingDoubleQueue {
    private final TxnInteger head, tail;
    private final TxnDouble[] queue;
    private final TxnInteger availableItems, availableSpaces;

    StmBlockingNDoubleQueue(){
        queue = Stream.generate(() -> newTxnDouble(0))
                .limit(50)
                .toArray(TxnDouble[]::new);
        head = newTxnInteger(0);
        tail = newTxnInteger(0);
        availableItems = newTxnInteger(0);
        availableSpaces = newTxnInteger(50);
    }

    @Override
    public double take()
    {
        return atomic(() -> {
            if(availableItems.get() == 0){
                retry();
                return 0.0; // Unused.
            } else {
                availableItems.decrement();
                double item = queue[head.get()].get();
                queue[head.get()].set(0.0);
                head.set((head.get() + 1) % queue.length);
                availableSpaces.increment();
                return item;
            }
        });
    }

    @Override
    public void put(double item)
    {
        atomic(() -> {
            if(availableSpaces.get() == 0){
                retry();
            } else {
                availableSpaces.decrement();
                queue[tail.get()].set(item);
                tail.set((tail.get() + 1) % queue.length);
                availableItems.increment();
            }
        });
    }
}

// ----------------------------------------------------------------------

class DoubleArray {
    public static double[] randomPermutation(int n) {
        double[] arr = fillDoubleArray(n);
        shuffle(arr);
        return arr;
    }

    private static double[] fillDoubleArray(int n) {
        double[] arr = new double[n];
        for (int i = 0; i < n; i++)
            arr[i] = i + 0.1;
        return arr;
    }

    private static final java.util.Random rnd = new java.util.Random();

    private static void shuffle(double[] arr) {
        for (int i = arr.length-1; i > 0; i--)
            swap(arr, i, rnd.nextInt(i+1));
    }

    // Swap arr[s] and arr[t]
    private static void swap(double[] arr, int s, int t) {
        double tmp = arr[s]; arr[s] = arr[t]; arr[t] = tmp;
    }

    // Minheap operations for parallel sort pipelines.
    // Minheap invariant:
    // If heap[0..k-1] is a minheap, then heap[(i-1)/2] <= heap[i] for
    // all indexes i=1..k-1.  Thus heap[0] is the smallest element.

    // Although stored in an array, the heap can be considered a tree
    // where each element heap[i] is a node and heap[(i-1)/2] is its
    // parent. Then heap[0] is the tree's root and a node heap[i] has
    // children heap[2*i+1] and heap[2*i+2] if these are in the heap.

    // In heap[0..k], move node heap[i] downwards by swapping it with
    // its smallest child until the heap invariant is reestablished.

    public static void minheapSiftdown(double[] heap, int i, int k) {
        int child = 2 * i + 1;
        if (child <= k) {
            if (child+1 <= k && heap[child] > heap[child+1])
                child++;
            if (heap[i] > heap[child]) {
                swap(heap, i, child);
                minheapSiftdown(heap, child, k);
            }
        }
    }

    // In heap[0..k], move node heap[i] upwards by swapping with its
    // parent until the heap invariant is reestablished.
    public static void minheapSiftup(double[] heap, int i, int k) {
        if (0 < i) {
            int parent = (i - 1) / 2;
            if (heap[i] < heap[parent]) {
                swap(heap, i, parent);
                minheapSiftup(heap, parent, k);
            }
        }
    }
}
