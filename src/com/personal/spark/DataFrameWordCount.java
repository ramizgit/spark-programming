package com.personal.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;

import static org.apache.spark.sql.functions.*;

public class DataFrameWordCount {

    public static void main(String[] args){

        //create spark session
        final SparkConf sparkConfig = new SparkConf().
                setAppName("word count").
                setMaster("local[2]").
                set("spark.driver.host", "localhost").
                set("spark.testing.memory", "2147480000");
        SparkContext sparkContext = new SparkContext(sparkConfig);
        SparkSession sparkSession = new SparkSession(sparkContext);

        //read input file
        Dataset<String> linesAsDataset = sparkSession.
                read().
                format("text").
                load("C:\\razarapersonal\\sparkprogram\\src\\main\\java\\com\\personal\\spark\\file.txt").
                as(Encoders.STRING());

        //lets print number of lines in the input
        System.out.println("line count : "+linesAsDataset.count());

        //split lines by space
        Dataset<String> wordsAsDataset = linesAsDataset.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) {
                return Arrays.asList(s.split(" ")).iterator();
            }
        }, Encoders.STRING());

        wordsAsDataset.show();

        //filter unwanted words
        /*Dataset<String> filteredDataset = wordsAsDataset.
                where(col("value").notEqual("!!!")).
                where(col("value").notEqual("and")).
                where(col("value").notEqual("it")).
                where(col("value").notEqual("is")).
                where(col("value").notEqual("a")).
                where(col("value").notEqual("as")).
                where(col("value").notEqual("i"));*/

        Dataset<String> filteredDataset = wordsAsDataset.where("value not in ('!!!','and','it','is','a','as','i')");

        //wordsAsDataset.where(col("value").equalTo("!!!").and(col("value").equalTo("").or(col("value").equalTo(""))));
        //wordsAsDataset.where(col("value").equalTo("!!!").or(col("value").equalTo("am")).or(col("value").equalTo("am")));

        //group by count and display as dataframe
        filteredDataset.groupBy("value").
                count().
                toDF("Word", "Count").
                //sort(col("Count").desc()).
                sort(desc("Count")).
                show();

        //-----------------------new way-----------------------------
        //read input file
        Dataset<Row> linesAsDf = sparkSession.
                read().
                format("text").
                load("C:\\razarapersonal\\sparkprogram\\src\\main\\java\\com\\personal\\spark\\file.txt");

        Dataset<Row> wordsDf = linesAsDf.select(explode(split(col("value"), " ")).alias("words"));
        Dataset<Row> filteredWordsDf = wordsDf.where("words not in ('!!!','and','it','is','a','as','i')");
        filteredWordsDf.groupBy("words").count().sort(desc("count")).show();
    }
}
