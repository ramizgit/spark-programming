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

public class DataFrameJoin {
    public static void main(String[] args){
        //spark context
        final SparkConf sparkConfig = new SparkConf().setAppName("word count").setMaster("local[2]").set("spark.driver.host", "localhost").set("spark.testing.memory", "2147480000");
        SparkContext sparkContext = new SparkContext(sparkConfig);
        SparkSession sparkSession = new SparkSession(sparkContext);

        List<StructField> empFields = Arrays.asList(
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false),
                DataTypes.createStructField("skills", DataTypes.StringType, false),
                DataTypes.createStructField("country", DataTypes.StringType, false)
        );

        List<StructField> empSalaryFields = Arrays.asList(
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("salary", DataTypes.StringType, false)
        );

        StructType empSchema = DataTypes.createStructType(empFields);
        StructType empSalarySchema = DataTypes.createStructType(empSalaryFields);

        Dataset<Row> employeeDf = sparkSession.read().option("multiline", "true").format("json").schema(empSchema).
                load("C:\\razarapersonal\\sparkprogram\\src\\main\\java\\com\\personal\\spark\\employee.json");

        employeeDf.printSchema();
        employeeDf.show();
        System.out.println("emp count : "+employeeDf.count());

        Dataset<Row> employeeSalaryDf = sparkSession.read().option("multiline", "true").format("json").schema(empSalarySchema).
                load("C:\\razarapersonal\\sparkprogram\\src\\main\\java\\com\\personal\\spark\\employeesalary.json");

        employeeSalaryDf.printSchema();
        employeeSalaryDf.show();
        System.out.println("emp salary count : "+employeeSalaryDf.count());

        //join
        employeeDf.join(employeeSalaryDf, employeeDf.col("name").equalTo(employeeSalaryDf.col("name")), "full").show();
    }
}

