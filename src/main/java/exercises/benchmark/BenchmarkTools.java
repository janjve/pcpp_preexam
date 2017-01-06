package exercises.benchmark;

import java.util.function.IntToDoubleFunction;

/**
 * Created by rrjan on 1/2/2017.
 */
public class BenchmarkTools
{
    // Examples
    private static void mathFunctionBenchmarks() {
        mark7("pow", i -> Math.pow(10.0, 0.1 * (i & 0xFF)));
        mark7("exp", i -> Math.exp(0.1 * (i & 0xFF)));
        mark7("log", i -> Math.log(0.1 + 0.1 * (i & 0xFF)));
        mark7("sin", i -> Math.sin(0.1 * (i & 0xFF)));
        mark7("cos", i -> Math.cos(0.1 * (i & 0xFF)));
        mark7("tan", i -> Math.tan(0.1 * (i & 0xFF)));
        mark7("asin", i -> Math.asin(1.0/256.0 * (i & 0xFF)));
        mark7("acos", i -> Math.acos(1.0/256.0 * (i & 0xFF)));
        mark7("atan", i -> Math.atan(1.0/256.0 * (i & 0xFF)));
    }

    public static double mark6(String msg, IntToDoubleFunction f) {
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
                double time = runningTime * 1e9 / count;
                st += time;
                sst += time * time;
                totalCount += count;
            }
            double mean = st/n, sdev = Math.sqrt((sst - mean*mean*n)/(n-1));
            System.out.printf("%-25s %15.1f ns %10.2f %10d%n", msg, mean, sdev, count);
        } while (runningTime < 0.25 && count < Integer.MAX_VALUE/2);
        return dummy / totalCount;
    }

    public static double mark7(String msg, IntToDoubleFunction f) {
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
                double time = runningTime * 1e6 / count; // microseconds
                st += time;
                sst += time * time;
                totalCount += count;
            }
        } while (runningTime < 0.25 && count < Integer.MAX_VALUE/2);
        double mean = st/n, sdev = Math.sqrt((sst - mean*mean*n)/(n-1));
        System.out.printf("%-25s %15.1f us %10.2f %10d%n", msg, mean, sdev, count);
        return dummy / totalCount;
    }

    // Same as Mark7, but takes in configuration parameters.
    public static double mark8(String msg, String info, IntToDoubleFunction f, int n, double minTime) {
        int count = 1, totalCount = 0;
        double dummy = 0.0, runningTime = 0.0, st = 0.0, sst = 0.0;
        do {
            count *= 2;
            st = sst = 0.0;
            for (int j=0; j<n; j++) {
                Timer t = new Timer();
                for (int i=0; i<count; i++)
                    dummy += f.applyAsDouble(i);
                runningTime = t.check();
                double time = runningTime * 1e9 / count;
                st += time;
                sst += time * time;
                totalCount += count;
            }
        } while (runningTime < minTime && count < Integer.MAX_VALUE/2);
        double mean = st/n, sdev = Math.sqrt((sst - mean*mean*n)/(n-1));
        System.out.printf("%-25s %s%15.1f ns %10.2f %10d%n", msg, info, mean, sdev, count);
        return dummy / totalCount;
    }

    public static double mark8(String msg, IntToDoubleFunction f) {
        return mark8(msg, "", f, 10, 0.25);
    }

    public static double mark8(String msg, String info, IntToDoubleFunction f) {
        return mark8(msg, info, f, 10, 0.25);
    }

    public static void systemInfo() {
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
}