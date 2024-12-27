import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.nio.file.Path;
import java.nio.file.Paths;
public class G003HW1 {
    public static void main(String[] args) throws IOException {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // Parameters are: num_colors, num_repeats, <path_to_file>
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (args.length != 3) {
            throw new IllegalArgumentException("USAGE: C R filepath");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        SparkConf conf = new SparkConf(true).setAppName("TriangleCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

       // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Reading input parameters and filename
        int C = Integer.parseInt(args[0]);
        int R = Integer.parseInt(args[1]);
        Path p = Paths.get(args[2]);
        String fileName = p.getFileName().toString();

        //Subdivide the input data it into K random partitions
        JavaRDD<String> rawData = sc.textFile(args[2]).repartition(C).cache();
        JavaRDD<Tuple2<Integer, Integer>> edges;

        //Parse the data from lines to edges (pairs of integers)
        edges = rawData.map((line)->
        {
            String[] temp = line.split(",");
            Tuple2<Integer, Integer> edge = new Tuple2(Integer.parseInt(temp[0]),Integer.parseInt(temp[1]));
            return edge;
        });

        //Print input size and parameters
        System.out.println("Dataset = "+fileName);
        System.out.println("Number of Edges = "+rawData.count());
        System.out.println("Number of Colors = "+C);
        System.out.println("Number of Repetitions = "+R);


        //===================ALGORITHM 1===============================
        long[] estimates = new long[R];
        double[] runningTimeInMilliseconds = new double[R];
        //for each repetition
        for (int ii=0;ii<R;ii++)
        {
            //get the starting time
            double startingTime = System.currentTimeMillis();
            //get the estimate number of triangles
            estimates[ii]= MR_ApproxTCwithNodeColors(edges, C);
            //get the finishing time
            double finishingTime = System.currentTimeMillis();
            //get elapsed time
            runningTimeInMilliseconds[ii]=finishingTime-startingTime;

        }

        //computing the median of the estimates and the average of the runtime
        double triangleMedian = myMedian(estimates);
        double avg = myAverage(runningTimeInMilliseconds);

        //printing outputs
        System.out.println("Approximation through node coloring");
        System.out.println("- Number of triangles (median over "+R+" runs) = "+((long)triangleMedian));
        System.out.println("- Running time (average over "+R+" runs) = "+avg+" ms");

        //=======ALGORITHM 2==================
        //get starting time
        double startAlg2 = System.currentTimeMillis();
        //get the estimate number of triangles
        long t2 = MR_ApproxTCwithSparkPartitions(edges, C);
        //get the finishing time
        double finishAlg2 = System.currentTimeMillis();
        //printing outputs
        System.out.println("Approximation through Spark partitions");
        System.out.println("- Number of triangles = " +t2);
        System.out.println("- Running time = "+(finishAlg2-startAlg2)+" ms");

    }

    //ALGORITHM 1
    public static long MR_ApproxTCwithNodeColors(JavaRDD<Tuple2<Integer, Integer>> edges, int C) {

        //generate the parameters
        Random random = new Random();
        int p = 8191;
        int a = random.nextInt(p - 1) + 1;
        int b = random.nextInt(p);

        //hash function
        Function<Integer, Integer> hC = u -> ((a * u + b) % p) % C;

        //==================ROUND 1==============================
        JavaPairRDD<Integer, Long> trianglesByColor = edges
                //map phase
                .filter(edge -> ((a * edge._1() + b) % p) % C == ((a * edge._2() + b) % p) % C) // filter different colors edges
                .mapToPair(edge -> new Tuple2<>(hC.call(edge._1()), edge)) // create new pair (key color , edge)
                //reduce phase
                .groupByKey() //group each color
                .mapValues(eList -> {   //create an array list of edges for each color and then call CountTriangles
                    ArrayList<Tuple2<Integer, Integer>> edgeArrayList = new ArrayList<Tuple2<Integer, Integer>>((Collection<? extends Tuple2<Integer, Integer>>) eList);
                    return CountTriangles(edgeArrayList);
                });

        //==========ROUND 2=====================
        long totalTriangleCount = trianglesByColor
                //map pahse
                .map(tuple -> tuple._2()) //keep only the triangle count for each color
                //reduce phase
                .reduce((x, y) -> x + y); //sum all triangle counts over all colors

        //approximate final value
        return C*C*totalTriangleCount;

    }

    //ALGORITHM 2
    public static long MR_ApproxTCwithSparkPartitions(JavaRDD<Tuple2<Integer, Integer>> edges, int C)
    {
        //===================ROUND 1==============================
        //map phase
        long totalTriangleCount = edges.mapPartitions((edgIter)->{
            ArrayList<Tuple2<Integer,Integer>> actualList = new ArrayList<>();
            while(edgIter.hasNext()) //generate an array list for each color
            {
                actualList.add(edgIter.next());
            }
            ArrayList<Long> finalCount = new ArrayList<>();
            finalCount.add(CountTriangles(actualList)); //generate list of counts
            return finalCount.iterator();})
        //reduce phase
        .reduce((x,y)->x+y); // sum of all triangle count

        //approximate final value
        return C*C*totalTriangleCount;
    }

    //given by the professor
    public static Long CountTriangles(ArrayList<Tuple2<Integer, Integer>> edgeSet) {
        if (edgeSet.size()<3) return 0L;
        HashMap<Integer, HashMap<Integer,Boolean>> adjacencyLists = new HashMap<>();
        for (int i = 0; i < edgeSet.size(); i++) {
            Tuple2<Integer,Integer> edge = edgeSet.get(i);
            int u = edge._1();
            int v = edge._2();
            HashMap<Integer,Boolean> uAdj = adjacencyLists.get(u);
            HashMap<Integer,Boolean> vAdj = adjacencyLists.get(v);
            if (uAdj == null) {uAdj = new HashMap<>();}
            uAdj.put(v,true);
            adjacencyLists.put(u,uAdj);
            if (vAdj == null) {vAdj = new HashMap<>();}
            vAdj.put(u,true);
            adjacencyLists.put(v,vAdj);
        }
        Long numTriangles = 0L;
        for (int u : adjacencyLists.keySet()) {
            HashMap<Integer,Boolean> uAdj = adjacencyLists.get(u);
            for (int v : uAdj.keySet()) {
                if (v>u) {
                    HashMap<Integer,Boolean> vAdj = adjacencyLists.get(v);
                    for (int w : vAdj.keySet()) {
                        if (w>v && (uAdj.get(w)!=null)) numTriangles++;
                    }
                }
            }
        }
        return numTriangles;
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

    //function to compute the average over the values of an array
    public static double myAverage(double[] estimates)
    {
        int length = estimates.length;
        double sum =0;
        for (int ii=0;ii<length;ii++)
        {
            sum=sum+estimates[ii];
        }
        return sum/length;
    }
}
