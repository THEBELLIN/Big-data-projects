import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.Random;

public class G003HW3 {

    // After how many items should we stop?
    public static final int THRESHOLD = 10000000;

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            throw new IllegalArgumentException("USAGE: D W left right K port");
        }
        // IMPORTANT: when running locally, it is *fundamental* that the
        // `master` setting is "local[*]" or "local[n]" with n > 1, otherwise
        // there will be no processor running the streaming computation and your
        // code will crash with an out of memory (because the input keeps accumulating).
        SparkConf conf = new SparkConf(true)
                .setMaster("local[*]") // remove this line if running on the cluster
                .setAppName("DistinctExample");

        // Here, with the duration you can control how large to make your batches.
        // Beware that the data generator we are using is very fast, so the suggestion
        // is to use batches of less than a second, otherwise you might exhaust the
        // JVM memory.
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(100));
        sc.sparkContext().setLogLevel("ERROR");

        // TECHNICAL DETAIL:
        // The streaming spark context and our code and the tasks that are spawned all
        // work concurrently. To ensure a clean shut down we use this semaphore.
        // The main thread will first acquire the only permit available and then try
        // to acquire another one right after spinning up the streaming computation.
        // The second tentative at acquiring the semaphore will make the main thread
        // wait on the call. Then, in the `foreachRDD` call, when the stopping condition
        // is met we release the semaphore, basically giving "green light" to the main
        // thread to shut down the computation.
        // We cannot call `sc.stop()` directly in `foreachRDD` because it might lead
        // to deadlocks.
        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        int D = Integer.parseInt(args[0]);
        int W = Integer.parseInt(args[1]);
        int left = Integer.parseInt(args[2]);
        int right = Integer.parseInt(args[3]);
        int K = Integer.parseInt(args[4]);
        int portExp = Integer.parseInt(args[5]);
        System.out.println("Receiving data from port = " + portExp);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        long[] streamLength = new long[1]; // Stream length (an array to be passed by reference)
        streamLength[0]=0L;
        HashMap<Long, Long> histogram = new HashMap<>(); // Hash Table for the distinct elements
        HashMap<Tuple2<Integer, Integer>,Long> sketchMatrix = new HashMap<>();

        Random random = new Random();
        int p = 8191;	 
        //int p = 22091; //this makes it work normally
        long a[] = new long[D];
        long b[] = new long[D];
        long c[] = new long[D];
        long d[] = new long[D];
        for(int i=0; i<D; i++)
        {
            a[i]=random.nextInt(p - 1) + 1;
            b[i]= random.nextInt(p);
            c[i]=random.nextInt(p - 1) + 1;
            d[i]= random.nextInt(p);
        }
        //hash function
        Function<Tuple2<Integer, Integer>, Integer> hC = u ->(int) (((a[u._1()] * u._2() + b[u._1()]) % p) % W);
        Function<Tuple2<Integer, Integer>, Integer> gC = u ->(int) (((((c[u._1()] * u._2() + d[u._1()]) % p) % 2)*2)-1);


        // CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)
                // For each batch, to the following.
                // BEWARE: the `foreachRDD` method has "at least once semantics", meaning
                // that the same data might be processed multiple times in case of failure.
                .foreachRDD((batch, time) -> {
                    // this is working on the batch at time `time`.
                    if (streamLength[0] < THRESHOLD) {
                        long batchSize = batch.count();
                        streamLength[0] += batchSize;
                        // Extract the distinct items from the batch
                        Map<Long, Long> batchItems = batch
                                .filter(s->(Integer.parseInt(s)>=left && Integer.parseInt(s)<=right))
                                .mapToPair(s -> new Tuple2<>(Long.parseLong(s), 1L))
                                .reduceByKey((i1, i2) -> i1 + i2)
                                .collectAsMap();
                        // Update the streaming state
                        for (Map.Entry<Long, Long> pair : batchItems.entrySet())
                        {
                            if(histogram.get(pair.getKey())!=null)
                            {
                                histogram.put(pair.getKey(), histogram.get(pair.getKey()) + pair.getValue());
                            }
                            else
                            {
                                histogram.put(pair.getKey(), pair.getValue());
                            }
                            for (int i =0;i<D;i++)
                            {
                                Tuple2<Integer, Integer> rowKey = new Tuple2<>(i, pair.getKey().intValue());
                                Tuple2<Integer, Integer> cell = new Tuple2<>(i, hC.call(rowKey));
                                sketchMatrix.put(cell, sketchMatrix.getOrDefault(cell, 0L) + (pair.getValue()*gC.call(rowKey)));
                            }
                        }

                        // If we wanted, here we could run some additional code on the global histogram
                        if (batchSize > 0) {
                            System.out.println("Batch size at time [" + time + "] is: " + batchSize);
                        }
                        if (streamLength[0] >= THRESHOLD) {
                            stoppingSemaphore.release();
                        }
                    }
                });

        // MANAGING STREAMING SPARK CONTEXT
        System.out.println("Starting streaming engine");
        sc.start();
        System.out.println("Waiting for shutdown condition");
        stoppingSemaphore.acquire();
        System.out.println("Stopping the streaming engine");
        // NOTE: You will see some data being processed even after the
        // shutdown command has been issued: This is because we are asking
        // to stop "gracefully", meaning that any outstanding work
        // will be done.
        sc.stop(false, false);
        System.out.println("Streaming engine stopped");

        // COMPUTE AND PRINT FINAL STATISTICS
        System.out.println("Number of items processed = " + streamLength[0]);
        System.out.println("Number of distinct items = " + histogram.size());
        long max = 0L;
        for (Long key : histogram.keySet()) {
            if (key > max) {max = key;}
            //System.out.println(key + ": " + histogram.get(key)); //debug
        }

        System.out.println("Largest item = " + max);
        double f2 =0;
        double sum=0;
        for(Long key :histogram.keySet())
        {
            f2+=histogram.get(key)*histogram.get(key);
            sum+=histogram.get(key);
        }

        double f2appr =0;
        for(Long key: histogram.keySet())
        {
            long counts[] = new long[D];
            for(int i=0; i<D; i++)
            {
                Tuple2<Integer, Integer> rowKey = new Tuple2<>(i, key.intValue());
                Tuple2<Integer, Integer> cell = new Tuple2<>(i, hC.call(rowKey));
                counts[i]=sketchMatrix.get(cell);
                counts[i]=counts[i]*counts[i];
            }
            double median = myMedian(counts);
            //System.out.println(key + " approx: " + median); //debug
            f2appr+=median;
        }

        System.out.println("number of processed items: " + sum);
        System.out.println("true F2 normalized: "+ (f2/(sum*sum)));
        System.out.println("approximate F2 normalized: "+ (f2appr/(sum*sum)));
    }

    //function to compute the median of an array of values
    public static double myMedian(long[] estimates)
    {
        int length = estimates.length;
        Arrays.sort(estimates);
        if(length%2==0) //even length
        {
            return 0.5*(estimates[length/2 - 1]+estimates[length/2]);
        }
        else
        {
            return estimates[(new Double(Math.floor(length/2))).intValue()];
        }
    }
}