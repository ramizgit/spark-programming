package com.personal.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;

public class DataFrameAndSqlOperations {
    public static void main(String[] args){
        //spark context
        final SparkConf sparkConfig = new SparkConf().setAppName("csv read").setMaster("local[2]").set("spark.driver.host", "localhost").set("spark.testing.memory", "2147480000");
        SparkContext sparkContext = new SparkContext(sparkConfig);
        SparkSession sparkSession = new SparkSession(sparkContext);

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false),
                DataTypes.createStructField("location", DataTypes.StringType, false),
                DataTypes.createStructField("salary", DataTypes.IntegerType, false)
        );

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> df = sparkSession.
                read().
                option("header", true).
                format("csv").
                schema(schema).
                load("C:\\razarapersonal\\sparkprogram\\src\\main\\java\\com\\personal\\spark\\employee.csv");

        df.printSchema();
        df.show();

        //-----------------sql operations on data frame------------------
        df.createOrReplaceTempView("emp_table");

        //project few columns
        sparkSession.sql("select age,salary from emp_table").show();
        df.select(df.col("age"), df.col("salary")).show();

        //select max salary
        sparkSession.sql("select max(salary) from emp_table").show();
        df.select(max(df.col("salary"))).show();

        //select emp name with max salary
        sparkSession.sql("select name from emp_table where salary = (select max(salary) from emp_table)").show();
        
        //groupby location
        sparkSession.sql("select location, count(1) from emp_table group by location").show();
        df.groupBy("location").count().show();
    }
}
