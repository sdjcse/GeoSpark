package org.datasyslab.geospark.spatialOperator;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by zulu9ner on 4/8/17.8**/

public class SpatialStatistics{
    public static int [][][] cube = new int[31][56][41];
    public static int N = 31 * 55 * 40;
    public static Tuple2<String, Integer> getValues(String s)
    {
        java.lang.String[] dataValue = s.split(",");
        double longitude = Double.parseDouble(dataValue[5]);

        double latitude = Double.parseDouble(dataValue[6]);
        double x = (-Math.round(longitude * 100.0) / 100.0);
        double y = Math.round(latitude * 100.0) / 100.0;
        int day = java.lang.Integer.parseInt((dataValue[1].split("\\s+|/|-"))[2]);
        java.lang.String key = x + "-" + y + "-" + day;
        if (latitude <= 40.9 && latitude >= 40.5 && longitude <= -73.7 && longitude >= -74.25)    {
            cube[day-1][(int)((x-73.7)*100)][(int)((y-40.5)*100)] = cube[day-1][(int)((x-73.7)*100)][(int)((y-40.5)*100)] + 1;
            return new Tuple2<>(key, 1);
        }
        else {
            return new Tuple2<>("default", 0);
        }
    }


    public static void SpatialStatistics(JavaSparkContext sc, String InputLocation,String pathFolder)
    {
        JavaRDD<String> dataFile = sc.textFile(InputLocation);
        JavaPairRDD<String, Integer> dataMaps = dataFile.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) {
                        return getValues(s);
                    }
                });
        dataMaps = dataMaps.filter(
                new Function<Tuple2<String, Integer>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Integer> stringTuple) throws Exception {
                        if (stringTuple._1().equals("default"))
                            return false;
                        return true;
                    }
                });
        JavaPairRDD<String, Integer> redDataMap  = dataMaps.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer val1, Integer val2) throws Exception {
                return val1+val2;
            }
        });

        Integer countTotal = redDataMap.values().reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer val1, Integer val2) throws Exception {
                return val1+val2;
            }
        });
        final Double average = (double)countTotal/N;
        System.out.println("Total Count : " + countTotal);
        System.out.println("Average : " + average);
        /*long flo = redDataMap.values().reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer val1, Integer val2) throws Exception {
                Double st = val2 - average;
                Double stDev = Math.abs(st) * Math.abs(st);
                return val1 + (int) Math.round(stDev*100);
            }
        });*/
        List<Integer> values = redDataMap.values().collect();
        Double sum2 = 0.0;
        for(Integer iter:values){
            Double stdev = Math.abs(iter - average);
            sum2 = sum2 + stdev * stdev;
        }
        sum2 = sum2 + (average*average)*(N-redDataMap.count());
        sum2 = sum2/N ; //redDataMap.count();
        Double stDev = Math.sqrt(sum2);
        System.out.println("Standard Deviation : " + stDev);
        System.out.println("Reduced Count = " + redDataMap.count());
        //redDataMap = redDataMap.repartition(1);
        //redDataMap.saveAsTextFile("~/DDS/file.txt");
        redDataMap = redDataMap.sortByKey();
        redDataMap.saveAsHadoopFile(pathFolder, String.class, Integer.class, TextOutputFormat.class);
        Map<String,Double> valueMap = new HashMap<>();
        for(int i=0;i<31;i++)
        {
            for (int j = 0; j < 56; j++)
            {
                for (int p = 0; p < 41; p++)
                {
                    int sum = 0;
                    int count = 0;
                    count++;
                    sum = sum + cube[i][j][p];
                    if (i != 0)
                    {
                        if(cube[i - 1][j][p]!=0)
                            count++;
                        sum = sum + cube[i - 1][j][p];
                        if (j != 0)
                        {
                            if(cube[i - 1][j - 1][p]!=0)
                                count++;
                            sum = sum + cube[i - 1][j - 1][p];
                            if (p != 0)
                            {
                                if(cube[i - 1][j - 1][p - 1]!=0)
                                    count++;
                                sum = sum + cube[i - 1][j - 1][p - 1];
                            }
                            if (p != 40)
                            {
                                if(cube[i - 1][j - 1][p + 1]!=0)
                                    count++;
                                sum = sum + cube[i - 1][j - 1][p + 1];
                            }
                        }
                        if (j != 55)
                        {
                            if(cube[i - 1][j + 1][p]!=0)
                            count++;
                            sum = sum + cube[i - 1][j + 1][p];
                            if (p != 0)
                            {
                                if(cube[i - 1][j + 1][p - 1]!=0)
                                    count++;
                                sum = sum + cube[i - 1][j + 1][p - 1];
                            }
                            if (p != 40)
                            {
                                if(cube[i - 1][j + 1][p + 1]!=0)
                                    count++;
                                sum = sum + cube[i - 1][j + 1][p + 1];
                            }
                        }
                        if (p != 0)
                        {
                            if(cube[i - 1][j][p - 1]!=0)
                                count++;
                            sum = sum + cube[i - 1][j][p - 1];
                        }
                        if (p != 40)
                        {
                            if(cube[i - 1][j][p + 1]!=0)
                                count++;
                            sum = sum + cube[i - 1][j][p + 1];
                        }
                    }
                    if (i != 30)
                    {
                        if(cube[i + 1][j][p]!=0)
                            count++;
                        sum = sum + cube[i + 1][j][p];
                        if (j != 0)
                        {
                            if(cube[i + 1][j - 1][p]!=0)
                                count++;
                            sum = sum + cube[i + 1][j - 1][p];
                            if (p != 0)
                            {
                                if(cube[i + 1][j - 1][p - 1]!=0)
                                    count++;
                                sum = sum + cube[i + 1][j - 1][p - 1];
                            }
                            if (p != 40)
                            {
                                if(cube[i + 1][j - 1][p + 1]!=0)
                                    count++;
                                sum = sum + cube[i + 1][j - 1][p + 1];
                            }
                        }
                        if (j != 55)
                        {
                            if(cube[i + 1][j + 1][p]!=0)
                                count++;
                            sum = sum + cube[i + 1][j + 1][p];
                            if (p != 0)
                            {
                                if(cube[i + 1][j + 1][p - 1]!=0)
                                    count++;
                                sum = sum + cube[i + 1][j + 1][p - 1];
                            }
                            if (p != 40)
                            {
                                if(cube[i + 1][j + 1][p + 1]!=0)
                                    count++;
                                sum = sum + cube[i + 1][j + 1][p + 1];
                            }
                        }
                        if (p != 0)
                        {
                            if(cube[i + 1][j][p - 1]!=0)
                                count++;
                            sum = sum + cube[i + 1][j][p - 1];
                        }
                        if (p != 40)
                        {
                            if(cube[i + 1][j][p + 1]!=0)
                                count++;
                            sum = sum + cube[i + 1][j][p + 1];
                        }
                    }
                    if (j != 0)
                    {
                        if(cube[i][j - 1][p]!=0)
                            count++;
                        sum = sum + cube[i][j - 1][p];
                        if (p != 0)
                        {
                            if(cube[i][j - 1][p - 1]!=0)
                                count++;
                            sum = sum + cube[i][j - 1][p - 1];
                        }
                        if (p != 40)
                        {
                            if(cube[i][j - 1][p + 1]!=0)
                                count++;
                            sum = sum + cube[i][j - 1][p + 1];
                        }
                    }
                    if (j != 55)
                    {
                        if(cube[i][j + 1][p]!=0)
                            count++;
                        sum = sum + cube[i][j + 1][p];
                        if (p != 0)
                        {
                            if(cube[i][j + 1][p - 1]!=0)
                                count++;
                            sum = sum + cube[i][j + 1][p - 1];
                        }
                        if (p != 40)
                        {
                            if(cube[i][j + 1][p + 1]!=0)
                                count++;
                            sum = sum + cube[i][j + 1][p + 1];
                        }
                    }
                    if (p != 0)
                    {
                        if(cube[i][j][p - 1]!=0)
                            count++;
                        sum = sum + cube[i][j][p - 1];
                    }
                    if (p != 40)
                    {
                        if(cube[i][j][p + 1]!=0)
                            count++;
                        sum = sum + cube[i][j][p + 1];
                    }
                    double numerator = sum - (average * count);
                    double denominator = (stDev * (Math.sqrt((count * N - count * count) / (N - 1))));
                    //double gScore = (cube[i][j][p] - (average * (double) sum)) / (stDev * (Math.sqrt(((double) sum * N - (double) sum * (double) sum) / (N - 1))));
                    double gScore = numerator/denominator;
                    valueMap.put(Double.toString(73.7 + (double)j / 100) + ',' + Double.toString(40.5 + (double)p / 100) + ',' + Integer.toString(i), gScore);
                }
            }
        }

        int cnt = 0;
        Map<String,Double> sortedMap = sortByComparator(valueMap,false);
        for (Map.Entry<String,Double> entry : sortedMap.entrySet()) {
            if(cnt > 49){
                break;
            }
            System.out.println(entry.getKey());
            System.out.println(entry.getValue());
            cnt++;
        }

    }

    private static Map<String, Double> sortByComparator(Map<String, Double> unsortMap, final boolean orderBool)
    {

        List<Map.Entry<String, Double>> list = new LinkedList<Map.Entry<String, Double>>(unsortMap.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Map.Entry<String, Double>>()
        {
            public int compare(Map.Entry<String, Double> o1,
                               Map.Entry<String, Double> o2)
            {
                if (orderBool)
                {
                    return o1.getValue().compareTo(o2.getValue());
                }
                else
                {
                    return o2.getValue().compareTo(o1.getValue());

                }
            }
        });

        // Maintaining insertion order with the help of LinkedList
        Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
        for (Map.Entry<String, Double> entry : list)
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }

}