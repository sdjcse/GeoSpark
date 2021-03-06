package org.datasyslab.geospark.spatialOperator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by zulu9ner on 4/8/17.8**/

public class SpatialStatistics2
{
    public static int N = 31 * 55 * 40;
    public static Tuple2<String, Integer> getValues(String s)
    {
        String[] dataValue = s.split(",");
        double longitude = Double.parseDouble(dataValue[5]);

        double latitude = Double.parseDouble(dataValue[6]);
        long x = (long) Math.floor(longitude * 100.0) * -1;
        long y = (long) Math.floor(latitude* 100.0 );
        int day = Integer.parseInt((dataValue[1].split("\\s+|/|-"))[2]);
        String key = y + "," + x + "," + day;
        if (latitude <= 40.9 && latitude >= 40.5 && longitude <= -73.7 && longitude >= -74.25)    {
            return new Tuple2<>(key, 1);
        }
        else {
            return new Tuple2<>("default", 0);
        }
    }
    private static List<String> getNeighbor(String coord) {
        String[] inputParams = coord.split(",");
        List<String> l = new ArrayList<>();
        double x1 = Double.parseDouble(inputParams[0]);
        double y1 = Double.parseDouble(inputParams[1]);
        int z1 = Integer.parseInt(inputParams[2]);
        l.add((int)x1+","+(int)y1+","+z1);
        for (int offsetX = -1; offsetX <= 1; offsetX++) {
            for (int offsetY = -1; offsetY <= 1; offsetY++) {
                for (int offsetZ = -1; offsetZ <= 1; offsetZ++) {
                    double x = x1;
                    double y = y1;
                    int z = z1;
                    x = x + offsetX;
                    y = y + offsetY;
                    z = z + offsetZ;
                    if (!((offsetX == 0) && (offsetY == 0) && (offsetZ == 0))) {
                        if ((x >= (40.5 * 100d) && x <= (40.9 * 100d))
                                && (y <= (74.25 * 100d) && y >= (73.7 * 100d))
                                && (z >= 1 && z <= 31)) {
                            l.add((int) x + "," + (int) y + "," + z);
                        }
                    }
                }
            }
        }
        return l;
    }

    public static void SpatialStatistics(JavaSparkContext sc, String InputLocation,String outFile)
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
        final Double average = ((double)countTotal)/N;
        System.out.println("Total Count : " + countTotal);
        System.out.println("Average : " + average);
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
        redDataMap = redDataMap.sortByKey();
        Map<String,Double> valueMap = new HashMap<>();
        Map<String,Integer> mapData = redDataMap.collectAsMap();
        for (Map.Entry<String, Integer> entry : mapData.entrySet())
        {
            List<String> neighbors;
            double sum = 0;
            neighbors = getNeighbor(entry.getKey());
            for(String st : neighbors)
            {
                sum = sum + ((mapData.get(st)!=null)?mapData.get(st):0);
            }
            double count = neighbors.size();
            double numerator = sum - (average * count);
            double denominator = (stDev * (Math.sqrt((count * N - count * count) / (N - 1))));
            double gScore = numerator/denominator;
            valueMap.put(entry.getKey(), gScore);
        }
        int cnt = 0;
        Map<String,Double> sortedMap = sortByComparator(valueMap,false);
        List<String> fileOut = new ArrayList<>();
        for (Map.Entry<String,Double> entry : sortedMap.entrySet()) {
            if(cnt > 49){
                break;
            }
            String[] finalFifty = entry.getKey().split(",");
            Double latitude = Double.parseDouble(finalFifty[0])/100;
            Double longitude = Double.parseDouble(finalFifty[1])/100;
            Integer day = Integer.parseInt(finalFifty[2])-1;
            System.out.println(latitude.toString()+",-"+longitude.toString()+","+day + "," + entry.getValue());
            fileOut.add(latitude.toString()+",-"+longitude.toString()+","+day + "," + entry.getValue());
            cnt++;
        }
        fileWriter(fileOut,outFile);
    }

    private static void fileWriter(List<String> arr,String fileName){
        try
        {
            Path out = Paths.get(fileName);
            Files.write(out,arr, Charset.defaultCharset());
        }catch (Exception e){
            System.out.println("Exception occured while writing to file!");
            e.printStackTrace();
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