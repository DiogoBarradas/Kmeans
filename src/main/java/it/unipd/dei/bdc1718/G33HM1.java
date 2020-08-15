package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Scanner;

public class G33HM1 {

    public static void main(String[] args) throws FileNotFoundException {
        if (args.length == 0) {
            throw new IllegalArgumentException("Expecting the file name on the command line");
        }

        // Read a list of numbers from the program options
        ArrayList<Double> lNumbers = new ArrayList<>();
        Scanner s =  new Scanner(new File(args[0]));
        while (s.hasNext()){
            lNumbers.add(Double.parseDouble(s.next()));
        }
        s.close();

        // Setup Spark
        SparkConf conf = new SparkConf(true)
                .setAppName("Preliminaries");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create a parallel collection
        JavaRDD<Double> dNumbers = sc.parallelize(lNumbers);


        double sum =dNumbers.reduce((x,y)->x+y);
        double total = dNumbers.count();
        double mean = sum /total;
        System.out.println("The mean is " + mean);

        ArrayList<Double> lMean = new ArrayList<>();
        lMean.add(mean);
        JavaRDD<Double> dMean = sc.parallelize(lMean);

       /* JavaRDD<Double> dDiffavgs = dNumbers.flatMap((x) ->{
            new Tuple2(double a, double b)

        }.union(dMean));
        /*JavaRDD<Double> diference = dNumbers.foreach((y) ->  Math.abs(x-y));
            return diference;*/
       /* System.out.println(dDiffavgs);

        /*Find min by reducing*/

        /*double minimum = dDiffavgs.reduce((x,y) -> {
            if(x<y)
                return x;
            else
                return y;
                });
        System.out.println(minimum);*/

        /*Filtering the numbers above mean*/

        JavaRDD<Double> greaterMean = dNumbers.filter((x) -> x > mean);

        System.out.println("The values above the mean are:");
        greaterMean.foreach((x)->{
            System.out.println(x);
        });






        /* min by function min */
        //double minimum2 = dDiffavgs.min();

        /*extra functions*/



        /*double sumOfSquares = dNumbers.map((x) -> x*x).reduce((x, y) -> x + y);
        System.out.println("The sum of squares is " + sumOfSquares);*/

    }

}
