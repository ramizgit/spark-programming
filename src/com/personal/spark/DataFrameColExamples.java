package com.personal.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;

public class DataFrameColExamples {
    public static void main(String[] args){
        //spark context
        final SparkConf sparkConfig = new SparkConf().
            setAppName("word count").
            setMaster("local[2]").
            set("spark.driver.host", "localhost").
            set("spark.testing.memory", "2147480000");
        
        SparkContext sparkContext = new SparkContext(sparkConfig);
        SparkSession sparkSession = new SparkSession(sparkContext);

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false),
                DataTypes.createStructField("skills", DataTypes.StringType, false),
                DataTypes.createStructField("country", DataTypes.StringType, false)
        );

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> employeeDf = sparkSession.
            read().
            option("multiline", "true").
            format("json").
            schema(schema).
            load("C:\\razarapersonal\\sparkprogram\\src\\main\\java\\com\\personal\\spark\\employee.json");

        employeeDf.printSchema();
        System.out.println("emp count : "+employeeDf.count());


        //update existing column
        Dataset<Row> newdataframe = employeeDf.withColumn("age", col("age").plus(100));
        newdataframe.printSchema();
        newdataframe.show();

        //add new column
        Dataset<Row> newdataframe2 = employeeDf.withColumn("address", col("country"));
        newdataframe2.printSchema();
        newdataframe2.show();

        //when function
        Dataset<Row> newdataframe3 = employeeDf.withColumn("isYoung", when(col("age").gt(20).and(col("age").lt(35)), 1).otherwise(0));
        newdataframe3.printSchema();
        newdataframe3.show();
        newdataframe3.explain();

        employeeDf.withColumn("isHighDemand", when(col("skills").contains("java").or(col("country").equalTo("US")), true).otherwise(false)).show();

        employeeDf.withColumn("isHighDemand", when(col("skills").contains("java"), true).otherwise(false)).
                filter("isHighDemand").
                show();

        employeeDf.withColumn("isHighDemand", when(col("skills").contains("java"), true).otherwise(false)).
                filter(not(col("isHighDemand"))).
                show();

        //column rename
        employeeDf.withColumnRenamed("name", "Name").show();

        //change name to upper case name
        employeeDf.withColumn("upper_name", upper(col("name"))).show();

        //add constant column
        employeeDf.withColumn("constant", lit(1)).show();

        //drop column
        employeeDf.drop("age").show();
    }
}
