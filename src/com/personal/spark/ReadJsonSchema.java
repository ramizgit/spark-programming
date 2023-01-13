package com.personal.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class ReadJsonSchema {

    public static void main(String[] args){
        //spark context
        final SparkConf sparkConfig = new SparkConf().setAppName("word count").setMaster("local[2]").set("spark.driver.host", "localhost").set("spark.testing.memory", "2147480000");
        SparkContext sparkContext = new SparkContext(sparkConfig);
        SparkSession sparkSession = new SparkSession(sparkContext);

        Dataset<Row> dataFrameWithoutInputSchema = sparkSession.
                read().
                option("multiline", "true").
                format("json").
                load("C:\\razarapersonal\\sparkprogram\\src\\main\\java\\com\\personal\\spark\\employee.json");

        dataFrameWithoutInputSchema.printSchema();
        dataFrameWithoutInputSchema.show();

        List<StructField> empFields = Arrays.asList(
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false),
                DataTypes.createStructField("skills", DataTypes.StringType, false),
                DataTypes.createStructField("country", DataTypes.StringType, false)
        );

        StructType empSchema = DataTypes.createStructType(empFields);

        Dataset<Row> dataFrameWithInputSchema = sparkSession.
                read().
                option("multiline", "true").
                format("json").
                schema(empSchema).
                load("C:\\razarapersonal\\sparkprogram\\src\\main\\java\\com\\personal\\spark\\employee.json");

        dataFrameWithInputSchema.printSchema();
        dataFrameWithInputSchema.show();
    }
}
