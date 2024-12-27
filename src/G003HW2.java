import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import scala.Tuple3;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.nio.file.Path;
import java.nio.file.Paths;
public class G003HW2 {
    public static void main(String[] args) throws IOException {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // Parameters are: num_partitions, <path_to_file>
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (args.length != 4) {
            throw new IllegalArgumentException("USAGE: C R F filepath");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        SparkConf conf = new SparkConf(true).setAppName("TriangleCount");
        conf.set("spark.locality.wait", "0s");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

       // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Reading input and filename to be printed
        int C = Integer.parseInt(args[0]);
        int R = Integer.parseInt(args[1]);
        int F = Integer.parseInt(args[2]);
        Path p = Paths.get(args[3]);
        //String fileName = p.getFileName().toString();

        // Read input file and subdivide it into 32 random partitions, as per our specifications
        JavaRDD<String> rawData = sc.textFile(args[3]).repartition(32).cache();
        JavaRDD<Tuple2<Integer, Integer>> edges;

        //Parse the data from lines to edges (pairs of integers)
        edges = rawData.map((line)->
        {
            String[] temp = line.split(",");
            Tuple2<Integer, Integer> edge = new Tuple2(Integer.parseInt(temp[0]),Integer.parseInt(temp[1]));
            //ArrayList<Tuple2<Integer, Tuple2<Integer, Integer>>> edges = new ArrayList<>();
            return edge;
        });

        //Print input size and parameters
        System.out.println("Dataset = "+p.toString());
        System.out.println("Number of Edges = "+rawData.count());
        System.out.println("Number of Colors = "+C);
        System.out.println("Number of Repetitions = "+R);


        //==============ALGORITHM 1===============================
        if(F==0) 
        {
            long[] estimates = new long[R];
            double[] runningTimeInMilliseconds = new double[R];

            //for each repetition
            for (int ii = 0; ii < R; ii++) 
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

            //compute the median of the estimates and the average of the runtime
            double triangleMedian = myMedian(estimates);
            double avg = myAverage(runningTimeInMilliseconds);

            //print outputs
            System.out.println("Approximation algorithm with node coloring");
            System.out.println("- Number of triangles (median over " + R + " runs) = " + ((long) triangleMedian));
            System.out.println("- Running time (average over " + R + " runs) = " + avg + " ms");
        }
        //=================ALGORITHM 2==================
        else if(F==1) 
        {            

            long[] trianglesNumbers = new long[R];
            double[] runningTimeInMilliseconds = new double[R];

            //for each repetition
            for (int ii = 0; ii < R; ii++) 
            {
            	//get the starting time
                double startingTime = System.currentTimeMillis();
                //get the exact number of triangles
                trianglesNumbers[ii] = MR_ExactTC(edges, C);
                //get the finishing time
                double finishingTime = System.currentTimeMillis();
                //get elapsed time
                runningTimeInMilliseconds[ii] = finishingTime - startingTime;
            }

            //compute the mean of the runtime
            double avg = myAverage(runningTimeInMilliseconds);

            //print outputs
            System.out.println("Exact algorithm with node coloring");
            System.out.println("- Number of triangles = " +trianglesNumbers[R-1]);
            System.out.println("- Running time (average over " + R + " runs) = " + avg + " ms");
        }
        //F != {0,1}
        else
        {
            throw new RuntimeException("Flag was not parsed correctly.");
        }

    }

    //ALGORITHM 1
    public static long MR_ApproxTCwithNodeColors(JavaRDD<Tuple2<Integer, Integer>> edges, int C) 
    {

    	//generate the parameters
        Random random = new Random();
        int p = 8191;
        long a = random.nextInt(p - 1) + 1;
        long b = random.nextInt(p);

        //hash function
        Function<Integer, Integer> hC = u ->(int) (((a * u + b) % p) % C);

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
    public static long MR_ExactTC(JavaRDD<Tuple2<Integer,Integer>> edges,int C)
    {
    	//generate the parameters
        Random random = new Random();
        int p = 8191;
        long a = random.nextInt(p - 1) + 1;
        long b = random.nextInt(p);

        //hash function
        Function<Integer, Integer> hC = u ->(int) (((a * u + b) % p) % C);

        //==================ROUND 1==============================
        JavaPairRDD<Tuple3<Integer,Integer,Integer>, Iterable<Tuple2<Integer,Integer>>> edgesByColor = edges
        		//map phase
                .flatMapToPair((edge) ->{
                	//Create Array list of pairs (key, edge)
                    ArrayList<Tuple2<Tuple3<Integer,Integer,Integer>,Tuple2<Integer,Integer>>> generatedByAnEdge = new ArrayList<>();
                    //for each possible third color of the triple
                    for(int ii=0; ii<C;ii++)
                    {
                    	//create color array
	                    Integer[] coloursToSort = new Integer[3];
	                    //first two colors are the edge's
	                    coloursToSort[0]= new Integer(hC.call(edge._1()));
	                    coloursToSort[1]= new Integer(hC.call(edge._2()));
	                    //third is dictated by the for loop
	                    coloursToSort[2]= new Integer(ii);
	                    //sort the colors so they are in non decreasing order
	                    Arrays.sort(coloursToSort);
	                    //create the triple "key"
	                    Tuple3<Integer, Integer, Integer> key = new Tuple3<>(coloursToSort[0],coloursToSort[1],coloursToSort[2]);
	                    //add pair (key, edge) to ArrayList
	                    generatedByAnEdge.add(new Tuple2<>(key, edge));
                    }
                    return generatedByAnEdge.iterator();
                })
                //reduce phase
                .groupByKey().cache(); //reduce by grouping the same keys together

        //==================ROUND 2==============================
        Long counts= edgesByColor
        		//map phase
                .map((pair)-> {
                	//create ArrayList with only the edges for each possible key combination
                    ArrayList<Tuple2<Integer,Integer>> edgesList= new ArrayList<>();
                    //discard the key and only add the edges
                    pair._2().forEach(edgesList::add);
                    //count triangles for each possible key value
                    return  CountTriangles2(edgesList,pair._1(),a,b,p,C);
                })
                //reduce phase
                .reduce((x,y)->x+y);	//sum all counts

        //return the exact value
        return counts;
    }

    //given by the professor
    public static Long CountTriangles2(ArrayList<Tuple2<Integer, Integer>> edgeSet, Tuple3<Integer, Integer, Integer> key, long a, long b, long p, int C) {
        if (edgeSet.size()<3) return 0L;
        HashMap<Integer, HashMap<Integer,Boolean>> adjacencyLists = new HashMap<>();
        HashMap<Integer, Integer> vertexColors = new HashMap<>();
        for (int i = 0; i < edgeSet.size(); i++) {
            Tuple2<Integer,Integer> edge = edgeSet.get(i);
            int u = edge._1();
            int v = edge._2();
            if (vertexColors.get(u) == null) {vertexColors.put(u, (int) ((a*u+b)%p)%C);}
            if (vertexColors.get(v) == null) {vertexColors.put(v, (int) ((a*v+b)%p)%C);}
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
                        if (w>v && (uAdj.get(w)!=null)) {
                            ArrayList<Integer> tcol = new ArrayList<>();
                            tcol.add(vertexColors.get(u));
                            tcol.add(vertexColors.get(v));
                            tcol.add(vertexColors.get(w));
                            Collections.sort(tcol);
                            boolean condition = (tcol.get(0).equals(key._1())) && (tcol.get(1).equals(key._2())) && (tcol.get(2).equals(key._3()));
                            if (condition) {numTriangles++;}
                        }
                    }
                }
            }
        }
        return numTriangles;
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
