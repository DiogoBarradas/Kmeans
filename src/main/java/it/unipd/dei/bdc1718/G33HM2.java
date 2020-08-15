package it.unipd.dei.bdc1718;


package it.unipd.dei.bdc1718;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


import java.io.IOException;
import java.util.*;

import static org.apache.spark.api.java.StorageLevels.MEMORY_AND_DISK;

    public class G33HM2 {

        public static void main(String[] args) throws IOException {

            long start;
            long end;
            Random random = new Random();


            if (args.length == 0) {
                throw new IllegalArgumentException("No input file given on command line.");
            }

            SparkConf configuration = new SparkConf(true)
                    .setAppName("Homework 2");

            JavaSparkContext sc = new JavaSparkContext(configuration);

            JavaRDD<String> docs = sc.textFile(args[0]).persist(MEMORY_AND_DISK);

            start = System.currentTimeMillis();
            JavaPairRDD<String, Long> wordcounts = docs
                    .flatMapToPair((document) -> {             // <-- Map phase
                        String[] tokens = document.split(" ");
                        ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                        for (String token : tokens) {
                            pairs.add(new Tuple2<>(token, 1L));
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
                    });
            end = System.currentTimeMillis();
            System.out.println("Elapsed time " + (end - start) + " ms. METHOD WRITTEN BY THE PROFESSOR. Press enter to finish");
            System.in.read();

            start = System.currentTimeMillis();
            JavaPairRDD<String, Long> wordcountsIWC1 = docs
                    .flatMapToPair((document) -> {             // <-- Map phase
                        String[] tokens = document.split(" ");
                        ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                        ArrayList<String> pairsstr = new ArrayList<>();
                        ArrayList<Long> pairslong = new ArrayList<>();
                        for (String token : tokens) {
                            if (pairsstr.contains(token)) {
                                int ind = pairsstr.indexOf(token);
                                Long gt = pairslong.get(ind) + 1;
                                pairslong.set(ind, gt);
                            } else {
                                pairsstr.add(token);
                                pairslong.add(1L);
                            }
                        }
                        for (int i = 0; i < pairsstr.size(); i++) {
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
                    });
                /*.mapToPair(item -> item.swap())// interchanges position of entries in each tuple
                .sortByKey(false, 1)
                .mapToPair(item -> item.swap());
        */
            end = System.currentTimeMillis();
            System.out.println("Elapsed time " + (end - start) + " ms. IMPROVED VERSION 1 ARRAYLISTS. Press enter to finish");
            System.in.read();

            start = System.currentTimeMillis();
            JavaPairRDD<String, Long> wordcountsIWC1HM = docs
                    .flatMapToPair((document) -> {             // <-- Map phase
                        String[] tokens = document.split(" ");
                        Map<String, Long> hm = new HashMap<String, Long>();
                        for (String token : tokens) {
                            if (hm.containsKey(token)) {
                                long cont = hm.get(token);
                                hm.put(token, cont + 1);
                            } else {
                                hm.put(token, 1L);
                            }
                        }

                        ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                        Iterator it = hm.entrySet().iterator();
                        while (it.hasNext()) {
                            Map.Entry pair = (Map.Entry) it.next();
                            String str = pair.getKey().toString();
                            Long l = Long.parseLong(pair.getValue().toString());
                            pairs.add(new Tuple2<String, Long>(str, l));
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
                /*.mapToPair(item -> item.swap())// interchanges position of entries in each tuple
                .sortByKey(false, 1)
                .mapToPair(item -> item.swap())*/;

            end = System.currentTimeMillis();
            System.out.println("Elapsed time " + (end - start) + " ms. IMPROVED VERSION 1 HM. Press enter to finish");
            System.in.read();

            start = System.currentTimeMillis();

            JavaPairRDD<String, Long> wordcountsIWC2 = docs
                    .flatMapToPair((document) -> {             // <-- Map phase
                        String[] tokens = document.split(" ");
                        Map<String, Long> hm = new HashMap<String, Long>();
                        for (String token : tokens) {
                            if (hm.containsKey(token)) {
                                long cont = hm.get(token);
                                hm.put(token, cont + 1);
                            } else {
                                hm.put(token, 1L);
                            }
                        }

                        ArrayList<Tuple2<Integer, Tuple2<String, Long>>> pairs = new ArrayList<>();
                        Iterator it = hm.entrySet().iterator();
                        while (it.hasNext()) {
                            Map.Entry pair = (Map.Entry) it.next();
                            String str = pair.getKey().toString();
                            Long l = Long.parseLong(pair.getValue().toString());
                            Integer r = random.nextInt(6);
                            pairs.add(new Tuple2<Integer, Tuple2<String, Long>>(r, new Tuple2<>(str, l)));
                        }

                        return pairs.iterator();
                    })
                    .groupByKey()
                    .flatMapToPair(v1 -> {
                        long sum = 0;
                        Map<String, Long> hm = new HashMap<String, Long>();
                        v1._2.forEach(stringLongTuple2 -> {
                            if (hm.containsKey(stringLongTuple2._1)) {
                                long cont = hm.get(stringLongTuple2._1);
                                hm.put(stringLongTuple2._1, cont + stringLongTuple2._2);
                            } else {
                                hm.put(stringLongTuple2._1, stringLongTuple2._2);
                            }
                        });

                        ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                        Iterator it = hm.entrySet().iterator();
                        while (it.hasNext()) {
                            Map.Entry pair = (Map.Entry) it.next();
                            String str = pair.getKey().toString();
                            Long l = Long.parseLong(pair.getValue().toString());
                            pairs.add(new Tuple2<String, Long>(str, l));
                        }
                        return pairs.iterator();
                    })
                    .groupByKey()
                    .mapValues((it) -> {
                        long sum = 0;
                        for (long c : it) {
                            sum += c;
                        }
                        return sum;
                    })
                /*.mapToPair(item -> item.swap())// interchanges position of entries in each tuple
                .sortByKey(false, 1)
                .mapToPair(item -> item.swap())*/;
            end = System.currentTimeMillis();
            System.out.println("Elapsed time " + (end - start) + " ms. IMPROVED VERSION 2. Press enter to finish");
            System.in.read();

            start = System.currentTimeMillis();
            //Reduce by key
            JavaPairRDD<String, Long> wordcountsRBK = docs
                    .flatMapToPair((document) -> {             // <-- Map phase
                        String[] tokens = document.split(" ");
                        ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                        for (String token : tokens) {
                            pairs.add(new Tuple2<>(token, 1L));
                        }
                        return pairs.iterator();
                    })
                    .reduceByKey((v1, v2) -> v1 + v2)
                /*.mapToPair(item -> item.swap())// interchanges position of entries in each tuple
                .sortByKey(false, 1)
                .mapToPair(item -> item.swap())*/;

            end = System.currentTimeMillis();
            System.out.println("Elapsed time " + (end - start) + " ms. REDUCE BY KEY. Press enter to finish");
            System.in.read();

            //JavaPairRDD<String, Long> filtered = wordcountsIWC1.filter(v1 -> v1._2 >= 10000);


        /*System.out.print("Insert the number of most occurrent elements you want to see: ");
        Scanner scan = new Scanner(System.in);
        boolean validData = false;
        int numberK = 0;
        do{
            try{
                numberK = scan.nextInt();//tries to get data. Goes to catch if invalid data
                validData = true;//if gets data successfully, sets boolean to true
            }catch(InputMismatchException e){
                //executes when this exception occurs
                System.out.println("Input has to be a number. ");
            }
        }while(validData==false);//loops until validData is true


        //wordcountsRBK.take(numberK).forEach(stringLongTuple2 -> System.out.println(stringLongTuple2._1 + " : " + stringLongTuple2._2));
        //wordcountsIWC1HM.take(numberK).forEach(stringLongTuple2 -> System.out.println(stringLongTuple2._1 + " : " + stringLongTuple2._2));
        //wordcountsIWC2.take(numberK).forEach(integerTuple2Tuple2 -> System.out.println(integerTuple2Tuple2._1 + " : " + integerTuple2Tuple2._2));

        // filter out words with fewer than threshold occurrences
        //JavaPairRDD<String, Long> filtered = wordcountsRBK.filter(v1 -> v1._2 >= 10000);

        //Print dDiffavgs
        //wordcounts.foreach(tuple2 -> System.out.println(tuple2._1 + " : " + tuple2._2));
        //wordcountsRBK.foreach(longTuple2 -> System.out.println(longTuple2._1 + " : " + longTuple2._2));
        //wordcountsIWC1.foreach(longTuple2 -> System.out.println(longTuple2._1 + " : " + longTuple2._2));
        //filtered.foreach(longTuple2 -> System.out.println(longTuple2._1 + " : " + longTuple2._2));

        System.out.println("Press enter to finish");

        System.in.read();*/

        }
    }
