package it.unipd.dei.bdc1718;

import com.beust.jcommander.converters.CommaParameterSplitter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

import static java.lang.Math.sqrt;

public class G33HM2d {


    public static void main(String[] args) throws IOException {
        /*if (args.length == 0) {
            throw new IllegalArgumentException("Expecting the file name on the command line");
        }*/

        //Spark Init

        SparkConf configuration = new SparkConf(true)
                .setAppName("WordCount")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(configuration);

        JavaRDD<String> lines = sc.textFile("test.txt").cache();
        lines.count();

        String option ;
        Scanner user_input = new Scanner( System.in );
        System.out.print("Enter your Option: " );
        option = user_input.next( );

        if (option.contentEquals("0")) {


            long startTime = System.currentTimeMillis();



            JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());



            /*JavaPairRDD<String, Long> pairs = words.mapToPair(s -> new Tuple2<>(s, words.filter(x -> x.equals(x)).count()));*/
           /* JavaPairRDD<String, Long> pairs = words.flatMapToPair(word-> {
                HashMap<String, Long> counter = new HashMap<String, Long>();

            });*/


            long endTime = System.currentTimeMillis();
            System.out.println("Elapsed time" + (endTime - startTime) + "ms");
            /*pairs.foreach(i -> System.out.println(i));*/
        }

        else if(option.contentEquals("1")){
            long startTime = System.currentTimeMillis();

            //Map Phase

            JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
            JavaPairRDD<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));

            //Reduce Phase
            JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

            long endTime = System.currentTimeMillis();
            System.out.println("Elapsed time" + (endTime - startTime) + "ms");
            /*counts.foreach(i -> System.out.println(i));*/

        }
        else if(option.contentEquals("2")){

            JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
            long size = words.count();
            int partition =(int) Math.ceil(Math.random()*Math.sqrt(size));

            JavaPairRDD<String, Long> wordcountsIWC1 = lines
                    .flatMapToPair((document) -> {             // <-- Map phase
                        String[] tokens = document.split(" ");
                        ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                        ArrayList<String> pairsstr = new ArrayList<>();
                        ArrayList<Long> pairslong = new ArrayList<>();
                        for (String token : tokens) {
                            if (pairsstr.contains(token)){
                                int ind = pairsstr.indexOf(token);
                                Long gt = pairslong.get(ind) + 1;
                                pairslong.set(ind,gt);
                            }
                            else {
                                pairsstr.add(token);
                                pairslong.add(1L);
                            }
                        }
                        for (int i=0; i<pairsstr.size(); i++){
                            pairs.add(new Tuple2<>(pairsstr.get(i), pairslong.get(i)));
                        }

                        return pairs.iterator();
                    })
                    .groupByKey()                       // <-- Reduce phase
                    .mapValues((it) -> {
                        long sum = 0;
                        for (long c : it) {
                            sum += c;
                        }
                        return sum;
                    })
                    .mapToPair(i -> i.swap())// interchanges position of entries in each tuple
                    .sortByKey(false, 1)
                    .mapToPair(i -> i.swap());



        }
        else {
            System.out.println("Expected correct input. Try again.");
        }

    }
}
