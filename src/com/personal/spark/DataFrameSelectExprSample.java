package com.personal.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class DataFrame2 {

    public static void main(String[] args){
        //spark context
        final SparkConf sparkConfig = new SparkConf().setAppName("word count").setMaster("local[2]").set("spark.driver.host", "localhost").set("spark.testing.memory", "2147480000");
        SparkContext sparkContext = new SparkContext(sparkConfig);
        SparkSession sparkSession = new SparkSession(sparkContext);

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false),
                DataTypes.createStructField("skills", DataTypes.StringType, false),
                DataTypes.createStructField("country", DataTypes.StringType, false)
        );

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> employeeDf = sparkSession.read().option("multiline", "true").format("json").schema(schema).
                load("C:\\razarapersonal\\sparkprogram\\src\\main\\java\\com\\personal\\spark\\employee.json");

        employeeDf.printSchema();
        System.out.println("emp count : "+employeeDf.count());

        employeeDf.select("name").show();
        employeeDf.select("name", "age").show();

        Dataset<Row> oldemp = employeeDf.where("age > 30"); //employeeDf.filter(employeeDf.col("age").gt(30)).show();

        Dataset<Row> youngemp = employeeDf.where("age <= 30"); //employeeDf.filter(employeeDf.col("age").lt(30)).show();

        oldemp.show();
        youngemp.show();

        employeeDf.selectExpr("sum(age)").show();
        employeeDf.selectExpr("avg(age)").show();

        employeeDf.selectExpr("*", "age > 30 as old").show();

       //partitions
        System.out.println("num of partitions : "+employeeDf.rdd().getNumPartitions());
        Dataset<Row> newdf = employeeDf.repartition(2, employeeDf.col("name"));
        System.out.println("num of partitions : "+newdf.rdd().getNumPartitions());

        //grp by
        employeeDf.groupBy("age").count().show();

        //dataframe distinct county
        employeeDf.selectExpr("country").distinct().show();

        //tmp table
        employeeDf.createOrReplaceTempView("emptmptable");
        Dataset<Row> sqldf = sparkSession.sql("select distinct(country) from emptmptable");
        sqldf.show();
    }
}
