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
        
        //------------Start : Projections-------------------
        dataFrameWithInputSchema.select("name", "age").show();
        dataFrameWithInputSchema.select(col("name"), col("skills")).show();
        dataFrameWithInputSchema.selectExpr("*", "age <= 30 as isYoung").show();
        //------------End : Projections-------------------

        //--------------Start : aggregations-----------------
        dataFrameWithInputSchema.select(max("Age")).show();
        dataFrameWithInputSchema.select(avg("Age")).show();
        dataFrameWithInputSchema.groupBy(col("country")).count().show();
        //--------------End : aggregations-----------------

        //-----------------Start : sql operations on the data frame------------------
        dataFrameWithInputSchema.createOrReplaceTempView("emp_table");

        //project few columns
        sparkSession.sql("select name, age from emp_table").show();

        //select max age
        sparkSession.sql("select max(age) from emp_table").show();

        //select average age
        sparkSession.sql("select avg(age) from emp_table").show();

        //select emp name with max salary
        sparkSession.sql("select name, age from emp_table where age >= (select avg(age) from emp_table)").show();

        //groupby location
        sparkSession.sql("select country, count(1) from emp_table group by country").show();

        //-----------------End : sql operations on the data frame------------------

        //--------------Start : withcolumn----------------
        dataFrameWithInputSchema.withColumn("isYoung", when(col("age").lt(30), true).otherwise(false)).show();
        //--------------Etart : withcolumn----------------
    }
}
