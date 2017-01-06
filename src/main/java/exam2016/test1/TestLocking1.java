package exam2016.test1;
public class TestLocking1 {
    public static void main(String[] args) {
        DoubleArrayList dal1 = new DoubleArrayList();
        dal1.add(42.1); dal1.add(7.2); dal1.add(9.3); dal1.add(13.4);
        dal1.set(2, 11.3);
        for (int i=0; i<dal1.size(); i++)
            System.out.println(dal1.get(i));
        DoubleArrayList dal2 = new DoubleArrayList();
        dal2.add(90.1); dal2.add(80.2); dal2.add(70.3); dal2.add(60.4); dal2.add(50.5);
        DoubleArrayList dal3 = new DoubleArrayList();
        testConcurrency();
    }

    public static void testConcurrency(){
        final int count = 1_000_000;
        final DoubleArrayList m = new DoubleArrayList();
        Runnable task = () -> {
            for (int i = 0; i < count; i++)
            {
                m.add(1);
            }};
        Thread t1 = new Thread(task);
        Thread t2 = new Thread(task);
        t1.start(); t2.start();
        try { t1.join(); t2.join(); } catch (InterruptedException exn) { }
        System.out.printf("Sum is %d and should be %d%n", m.size(), 2 * count);
    }
}

// Expandable array list of doubles.

class DoubleArrayList {
    // Invariant: 0 <= size <= items.length
    private double[] items = new double[2];
    private volatile int size = 0;
    private final Object slock = new Object();
    private final Object ilock = new Object();

    // Number of items in the double list
    public int size() {
        return size;
    }

    // Return item number i, if any
    public double get(int i) {
        synchronized (ilock){
            if (0 <= i && i < size)
                return items[i];
            else
                throw new IndexOutOfBoundsException(String.valueOf(i));
        }
    }

    // Add item x to end of list
    public boolean add(double x) {
        synchronized (ilock){
        synchronized (slock){
            if (size == items.length) {
                double[] newItems = new double[items.length * 2];
                for (int i=0; i<items.length; i++)
                    newItems[i] = items[i];
                items = newItems;
            }
            items[size] = x;
            size++;
            return true;
        }}
    }

    // Replace item number i, if any, with x
    public double set(int i, double x) {
        synchronized (ilock){
            if (0 <= i && i < size) {
                double old = items[i];
                items[i] = x;
                return old;
            } else
                throw new IndexOutOfBoundsException(String.valueOf(i));
        }
    }

    // The double list formatted as eg "[3.2, 4.7]"
    public String toString() {
        synchronized (ilock){
            StringBuilder sb = new StringBuilder("[");
            for (int i=0; i<size; i++)
                sb.append(i > 0 ? ", " : "").append(items[i]);
            return sb.append("]").toString();
        }
    }
}
