package org.dds.mainpack;

import org.apache.commons.io.input.ReaderInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.spatialOperator.SpatialStatistics2;

import java.io.*;
import java.net.URI;

/**
 * Created by sdj on 4/16/17.
 */
public class MainClass
{
    static String hdfsFileName = "hdfs://master:54310/data.csv";
    public static void main(String[] args)
    {
        String src = args[0];
        String dest = args[1];
        JavaSparkContext scObject = new JavaSparkContext(new SparkConf().setAppName("spatialStat"));
        new MainClass().loader(src,hdfsFileName);
        SpatialStatistics2.SpatialStatistics(scObject,hdfsFileName,dest);
    }



    // The function gives InputStream Object after ignoring the first line of the given input file
    private InputStream streamFetcher(String local){
        InputStream in;
        BufferedReader reader;
        InputStream finalInp = null;
        try
        {
             in = new BufferedInputStream(new FileInputStream(local));
            reader = new BufferedReader(new InputStreamReader(in));
            reader.readLine();
            finalInp = new ReaderInputStream(reader);

        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
        } catch (IOException e)
        {
            e.printStackTrace();
        }
        return finalInp;
    }

    private void loader(String src,String destination){
        InputStream inpObject = streamFetcher(src);
        Configuration confObject = new Configuration();
        FileSystem fsObject;
        OutputStream outObject;
        try
        {
             fsObject = FileSystem.get(URI.create(destination),confObject);
             outObject = fsObject.create(new Path(destination));
            IOUtils.copyBytes(inpObject,outObject,4096,true);

        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
