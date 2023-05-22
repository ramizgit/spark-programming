package com.personal.spark.window;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import static org.apache.spark.sql.functions.*;

public class SparkWindowOperations {

    public static void main(String[] args)
    {
        //spark context
        final SparkConf sparkConfig = new SparkConf().
                setAppName("csv read").
                setMaster("local[2]").
                set("spark.driver.host", "localhost").
                set("spark.testing.memory", "2147480000");
        SparkContext sparkContext = new SparkContext(sparkConfig);
        SparkSession sparkSession = new SparkSession(sparkContext);

        Dataset<Row> df = sparkSession.
                read().
                option("header", true).
                format("csv").
                load("C:\\razarapersonal\\sparkprogram\\src\\main\\java\\com\\personal\\spark\\window\\input.csv");

        df.printSchema();
        df.show();

        WindowSpec windowSpec = Window.
                partitionBy("division"). //group by columns go here
                orderBy(col("salary").desc()). //sorting column and order
                rowsBetween(Window.unboundedPreceding(), Window.currentRow()); //row frame or range frame specification
        
        df.select(col("name"),
                col("location"),
                col("division"),
                col("salary"),
                dense_rank().over(windowSpec).alias("salary_rank"),
                max("salary").over(windowSpec).alias("department_max_salary"),
                max("salary").over(windowSpec).minus(col("salary")).alias("diff_with_department_max_salary")
                ).show();
    }
}
