package com.personal.spark.window;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SparkWindowRangeFunction {
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

        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("location", DataTypes.StringType, false),
                DataTypes.createStructField("division", DataTypes.StringType, false),
                DataTypes.createStructField("salary", DataTypes.IntegerType, false)
        );

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> df = sparkSession.
                read().
                format("csv").
                option("header", true).
                schema(schema).
                load("C:\\razarapersonal\\sparkprogram\\src\\main\\java\\com\\personal\\spark\\window\\input.csv");

        df.printSchema();
        df.show();

        WindowSpec divWindowSpec = Window.
                partitionBy(col("division")).
                orderBy(col("salary").desc()).
                rangeBetween(-15000, 1000);
                //rowsBetween(Window.unboundedPreceding(), Window.currentRow());

        Dataset<Row> df2 = df.
                withColumn("department_max_salary", max("salary").over(divWindowSpec));
        df2.show();

    }
}

