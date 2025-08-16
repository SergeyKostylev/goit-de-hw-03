from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def hw3():
    spark = SparkSession.builder.appName("Pi").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Завантажте та прочитайте кожен CSV-файл як окремий DataFrame.
    products = getProducts(spark)
    purchases = getPurchases(spark)
    users = getUsers(spark)

    products.show(2)
    purchases.show(2)
    users.show(2)

    #2. Очистіть дані, видаляючи будь - які рядки з пропущеними значеннями.
    print("Products amount:", products.count())
    print("Purchases amount:", purchases.count())
    print("Users amount:", users.count())

    products = products.dropna()
    purchases = purchases.dropna()
    users = users.dropna()

    print("Products amount after cleaning:", products.count())
    print("Purchases amount after cleaning:", purchases.count())
    print("Users amount after cleaning:", users.count())

    # 3 Визначте загальну суму покупок за кожною категорією продуктів.

    purchases_with_products = purchases.join(products, on="product_id", how="inner")
    purchases_with_products = purchases_with_products.withColumn(
        "total_price", F.col("price") * F.col("quantity")
    )
    category_totals = purchases_with_products.groupBy("category").agg(
        F.round(F.sum("total_price"), 2).alias("total_amount")
    )
    category_totals.show()

    # 4. Визначте суму покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно.

    filtered_users = users.filter((F.col("age") >= 18) & (F.col("age") <= 25))
    purchases_with_filtered_users = purchases.join(filtered_users, on="user_id", how="inner")

    purchases_with_products = purchases_with_filtered_users.join(products, on="product_id", how="inner")

    category_totals_filtered_users = purchases_with_products.groupBy("category").agg(
        F.round(F.sum(F.col("price") * F.col("quantity")), 2).alias("total_amount")
    )

    print('category_totals_filtered_user:')
    category_totals_filtered_users.show()

    # 5. Визначте частку покупок за кожною категорією товарів від сумарних витрат для вікової категорії від 18 до 25 років.
    category_totals_filtered_users_5 = (purchases.join(filtered_users, on="user_id", how="inner")
                               .join(products, on="product_id", how="inner")
                               .withColumn("total_price", F.col("price") * F.col("quantity"))
                                .groupBy("category").agg(
                                    F.round(F.sum("total_price"), 2).alias("total_amount")
                                ))

    total_sum = category_totals_filtered_users_5.agg(F.sum("total_amount").alias("total_sum")).collect()[0]["total_sum"]

    category_totals_filtered_users_5 = category_totals_filtered_users_5.withColumn(
        "percent", F.round(F.col("total_amount") / total_sum * 100, 2)
    )

    print("--- 5 --- category_totals_filtered_users:")
    category_totals_filtered_users_5.show()

    # 6. Виберіть 3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років.

    top3_categories = category_totals_filtered_users_5.orderBy(F.col("percent").desc()).limit(3)
    print("--- 6 --- top3_categories:")
    top3_categories.show()



    spark.stop()


def getProducts(spark):
    return (spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv('hw3/products.csv'))

def getPurchases(spark):
    return (spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv('hw3/purchases.csv'))

def getUsers(spark):
    return (spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv('hw3/users.csv'))