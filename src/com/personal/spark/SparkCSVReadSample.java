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

public class SparkCSVReadSample {
    public static void main(String[] args){
        //spark context
        final SparkConf sparkConfig = new SparkConf().setAppName("csv read").setMaster("local[2]").set("spark.driver.host", "localhost").set("spark.testing.memory", "2147480000");
        SparkContext sparkContext = new SparkContext(sparkConfig);
        SparkSession sparkSession = new SparkSession(sparkContext);

        Dataset<Row> df = sparkSession.
                read().
                option("header", true).
                format("csv").
                load("C:\\razarapersonal\\sparkprogram\\src\\main\\java\\com\\personal\\spark\\employee.csv");

        df.printSchema();
        df.show();

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false),
                DataTypes.createStructField("location", DataTypes.StringType, false)
        );

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> df2 = sparkSession.
                read().
                option("header", true).
                format("csv").
                schema(schema).
                load("C:\\razarapersonal\\sparkprogram\\src\\main\\java\\com\\personal\\spark\\employee.csv");

        df2.printSchema();
        df2.show();

        //write
        df2.write().format("csv").save("C:\\razarapersonal\\sparkprogram\\src\\main\\java\\com\\personal\\spark\\employeeFromDF.csv");
    }
}
