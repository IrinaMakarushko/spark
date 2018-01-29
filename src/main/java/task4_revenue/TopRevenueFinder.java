package task4_revenue;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.desc;

public class TopRevenueFinder {

    public static final int TOP_N = 3;
    public static final String ORDER_ITEM_FILENAME = "src\\main\\resources\\order_item.json";
    public static final String PRODUCT_FILENAME = "src\\main\\resources\\product.json";

    public static void main(String[] args) {
        SparkSession sparkSession = getSparkSession();
        Dataset<Row> orderItems = sparkSession.read().json(ORDER_ITEM_FILENAME);
        Dataset<Row> product = sparkSession.read().json(PRODUCT_FILENAME);

        //Revenue is cost * qty
        Column cost = new Column("cost");
        Column qty = new Column("qty");
        Column revenue = cost.multiply(qty);

        //Join orderItems and products by product_id and add column revenue
        Dataset<Row> joiningProductsAndOrderItems = product.join(orderItems, "product_id").withColumn("revenue", revenue);

        //Rank in groups by category_id that ordered by revenue
        WindowSpec groupByCategoryOrderByRevenue = Window.partitionBy("category_id").orderBy(desc("revenue"));
        Dataset<Row> rankRows = joiningProductsAndOrderItems
                .withColumn("rank", functions.rank().over(groupByCategoryOrderByRevenue))
                .where("rank <= " + TOP_N);

        rankRows.show();
        sparkSession.close();
    }

    private static SparkSession getSparkSession() {
        System.setProperty("hadoop.home.dir", "C:/winutils/");
        SparkSession sparkSession = SparkSession
                .builder()
                .master("local")
                .appName("Top Revenue")
                .getOrCreate();
        return sparkSession;
    }
}
