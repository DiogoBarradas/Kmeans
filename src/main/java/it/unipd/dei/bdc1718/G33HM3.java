package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector ;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.apache.spark.mllib.linalg.Vectors.dense;

public class  G33HM3 {

    public static void main(String[] args) throws FileNotFoundException {
        if (args.length == 0) {
            throw new IllegalArgumentException("Expecting the file name on the command line");
        }

        // Setup Spark
        /*SparkConf conf = new SparkConf(true)
                .setAppName("Preliminaries");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create a parallel collection
        JavaRDD<Double> dNumbers = sc.parallelize(lNumbers);*/
        try{
            ArrayList<Vector> P = InputOutput.readVectorsSeq("vecs-50-10000.txt");
            int k = 5;
            int k1= 8;
            long startTime = System.currentTimeMillis();
            kcenter(P,k);
            long endTime = System.currentTimeMillis();
            /* print kcenter running time*/
            System.out.println("Elapsed time" + (endTime - startTime) + "ms");

            /*fill he weighs vector all the same=1*/
            long a = 1;
            ArrayList<Long> WP = new ArrayList<Long>(Collections.nCopies(P.size(),a));

            kmeansObj(P,kmeansPP(P,WP,k));

            /*same procedure, now with bigger k1>k (better centers obtained)*/
            kcenter(P,k1);
            kmeansObj(P,kmeansPP(P,WP,k));



        }
        catch (IOException e) {
            e.printStackTrace();
        }




}

    public static ArrayList<Vector> kcenter(ArrayList<Vector> P , int k){



        ArrayList<Vector> C = new ArrayList<>();
        for (int i=1; i<=k; i++){
            /*pick a random index and add it to k centers group C*/
            if (i==1){
                Random randomGenerator = new Random();
                int index = randomGenerator.nextInt(P.size());
                C.add(P.get(index));
                P.remove(P.get(index));
            }
            /*discovers the vector in P with maximum distance to all the center points (C)*/
            else{
                double maxmindist = 0;
                double mindist;
                int index = 0;
                for (int j = 0; j < P.size(); j++){
                    mindist = Double.POSITIVE_INFINITY;
                    for (int l = 0; l < C.size(); l++){
                        double distance = Vectors.sqdist(P.get(j),C.get(l));
                        if(distance < mindist){
                            mindist = distance;
                        }
                    }
                    if (mindist > maxmindist){
                        index = j;
                        maxmindist = mindist;
                    }
                }
                C.add(P.get(index));
                P.remove(P.get(index));
            }
        }
        return C;
    }

    public static ArrayList<Vector> kmeansPP(ArrayList<Vector> P ,ArrayList<Long> WP, int k){

        ArrayList<Vector> C = new ArrayList<>();
        for (int i=1; i<k; i++){
            /*pick a random index and add it to k centers group C*/
            if (i==1){
                Random randomGenerator = new Random();
                int index = randomGenerator.nextInt(P.size());
                C.add(P.get(index));
                P.remove(P.get(index));

            }
            else{
                /*compute dp*/
                double sumdist=0;
                for (int j=0; j<P.size(); j++) {
                    double mindist =0;
                    for (int l = 0; l < C.size(); l++) {
                        double distance = Vectors.sqdist(P.get(j), C.get(l));
                        if(distance<mindist){
                            mindist=distance;
                        }
                        sumdist=sumdist+distance;

                    }
                    long mindistl = (new Double(mindist)).longValue();
                    long sumdistl = (new Double(sumdist)).longValue();
                    long probability = (mindistl*mindistl) / (sumdistl*sumdistl);
                    WP.add(probability);
                }
                Random randomGenerator = new Random();
                int index = randomGenerator.nextInt(P.size())/* *WP.get(index)*/;


                /* como faço uma escolha random com aquela certa probabilidade desse vector ?*/
                C.add(P.get(index));
                P.remove(P.get(index));
            }

        }
            return C;

    }

    public static double kmeansObj(ArrayList<Vector> P, ArrayList<Vector> C ){
        double sum =0;
        for (int j=0; j<P.size(); j++) {
            double mindist =0;
            for (int l = 0; l < C.size(); l++) {
                double distance = Vectors.sqdist(P.get(j), C.get(l));
                if (distance < mindist) {
                    mindist = distance;
                }
            }
            sum=sum+mindist;
        }
        return sum / (P.size());
    }

}
