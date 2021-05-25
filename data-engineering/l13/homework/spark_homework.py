# %% Import deps
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# %% Configuration cell
pg_driver_path = "/opt/spark/drivers/postgresql-42.2.20.jar"
pg_url = "jdbc:postgresql://localhost:5432/pagila"
pg_creds = {"user": "pguser", "password": "secret"}
table_actor_name = "public.actor"
table_address_name = "public.address"
table_category_name = "public.category"
table_city_name = "public.city"
table_coutry_name = "public.coutry"
table_customer_name = "public.customer"
table_film_name = "public.film"
table_film_actor_name = "public.film_actor"
table_film_category_name = "public.film_category"
table_inventory_name = "public.inventory"
table_language_name = "public.language"
table_payment_name = "public.payment"
table_rental_name = "public.rental"
table_staff_name = "public.staff"
table_store_name = "public.store"

# %% Create Spark session
spark = SparkSession.builder \
    .config("spark.driver.extraClassPath", pg_driver_path) \
    .master("local") \
    .appName("L13").getOrCreate()

# %% Just test if everything is good
spark.version

# %% вывести количество фильмов в каждой категории, отсортировать по убыванию.
cat_df = spark.read.jdbc(pg_url, table_category_name, properties=pg_creds)
film_cat_df = spark.read.jdbc(pg_url, table_film_category_name, properties=pg_creds)
film_df = spark.read.jdbc(pg_url, table_film_name, properties=pg_creds)

cat_df \
    .join(film_cat_df, on="category_id") \
    .join(film_df, on="film_id") \
    .groupBy(cat_df.category_id, cat_df.name) \
    .agg(F.count(cat_df.category_id).alias("film_count")) \
    .sort(cat_df.category_id) \
    .select(cat_df.name, "film_count") \
    .show()

cat_df.unpersist()
film_cat_df.unpersist()
film_df.unpersist()

# %% вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
actor_df = spark.read.jdbc(pg_url, table_actor_name, properties=pg_creds)
film_act_df = spark.read.jdbc(pg_url, table_film_actor_name, properties=pg_creds)
inv_df = spark.read.jdbc(pg_url, table_inventory_name, properties=pg_creds)
rental_df = spark.read.jdbc(pg_url, table_rental_name, properties=pg_creds)

actor_df \
    .join(film_act_df, on="actor_id") \
    .join(inv_df, on="film_id") \
    .join(rental_df, on="inventory_id") \
    .groupBy(actor_df.actor_id \
           , actor_df.first_name \
           , actor_df.last_name) \
    .agg(F.count(rental_df.rental_id).alias("rental_count")) \
    .sort("rental_count", ascending=False) \
    .limit(10) \
    .select(actor_df.first_name, actor_df.last_name, "rental_count") \
    .show()

actor_df.unpersist()
film_act_df.unpersist()
inv_df.unpersist()
rental_df.unpersist()

# %% вывести категорию фильмов, на которую потратили больше всего денег.
cat_df = spark.read.jdbc(pg_url, table_category_name, properties=pg_creds)
film_cat_df = spark.read.jdbc(pg_url, table_film_category_name, properties=pg_creds)
inv_df = spark.read.jdbc(pg_url, table_inventory_name, properties=pg_creds)
rental_df = spark.read.jdbc(pg_url, table_rental_name, properties=pg_creds)
pay_df = spark.read.jdbc(pg_url, table_payment_name, properties=pg_creds)

cat_df \
    .join(film_cat_df, on="category_id") \
    .join(inv_df, on="film_id") \
    .join(rental_df, on="inventory_id") \
    .join(pay_df, on="rental_id") \
    .groupBy(cat_df.category_id, cat_df.name) \
    .agg(F.sum(pay_df.amount).alias("total_amount")) \
    .sort("total_amount", ascending=False) \
    .limit(1) \
    .select(cat_df.name, "total_amount") \
    .show()

cat_df.unpersist()
film_cat_df.unpersist()
inv_df.unpersist()
rental_df.unpersist()
pay_df.unpersist()

# %% вывести названия фильмов, которых нет в inventory.
film_df = spark.read.jdbc(pg_url, table_film_name, properties=pg_creds)
inv_df = spark.read.jdbc(pg_url, table_inventory_name, properties=pg_creds)

film_df \
    .join(inv_df, on="film_id", how="left_anti") \
    .select(film_df.film_id, film_df.title) \
    .sort(film_df.film_id) \
    .show()

film_df.unpersist()
inv_df.unpersist()

# %% вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
actor_df = spark.read.jdbc(pg_url, table_actor_name, properties=pg_creds)
film_act_df = spark.read.jdbc(pg_url, table_film_actor_name, properties=pg_creds)
film_cat_df = spark.read.jdbc(pg_url, table_film_category_name, properties=pg_creds)
cat_df = spark.read.jdbc(pg_url, table_category_name, properties=pg_creds)

child_category_ordered_actors_df = actor_df \
    .join(film_act_df, on="actor_id") \
    .join(film_cat_df, on="film_id") \
    .join(cat_df, on="category_id") \
    .filter(cat_df.name == 'Children') \
    .groupBy(actor_df.actor_id \
           , actor_df.first_name \
           , actor_df.last_name) \
    .agg(F.count(actor_df.actor_id).alias("film_count")) \
    .sort("film_count", ascending=False)

third_film_count = child_category_ordered_actors_df.collect()[3].film_count

child_category_ordered_actors_df \
    .select("first_name", "last_name", "film_count") \
    .filter(child_category_ordered_actors_df.film_count >= third_film_count) \
    .show()

actor_df.unpersist()
film_act_df.unpersist()
film_cat_df.unpersist()
cat_df.unpersist()


# %% вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.
city_df = spark.read.jdbc(pg_url, table_city_name, properties=pg_creds)
address_df = spark.read.jdbc(pg_url, table_address_name, properties=pg_creds)
customer_df = spark.read.jdbc(pg_url, table_customer_name, properties=pg_creds)

active_passive_df = city_df \
    .join(address_df, on="city_id") \
    .join(customer_df, on="address_id") \
    .groupBy(city_df.city_id, city_df.city, customer_df.active) \
    .agg(F.count(city_df.city_id).alias("customer_count"))

active_df = active_passive_df \
    .filter(active_passive_df.active == 1) \
    .withColumnRenamed("customer_count", "active_customer_count")

passive_df = active_passive_df \
    .filter(active_passive_df.active != 1) \
    .withColumnRenamed("customer_count", "passive_customer_count") \

city_df \
    .withColumnRenamed("city", "city_name") \
    .join(passive_df, on="city_id", how="left") \
    .join(active_df, on="city_id", how="left") \
    .select("city_name" \
          , F.coalesce(passive_df.passive_customer_count, F.lit(0)).alias("passive") \
          , F.coalesce(active_df.active_customer_count, F.lit(0)).alias("active")) \
    .sort("passive", "active", ascending=False) \
    .show()

city_df.unpersist()
address_df.unpersist()
customer_df.unpersist()

# %% вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”.
cat_df = spark.read.jdbc(pg_url, table_category_name, properties=pg_creds)
film_cat_df = spark.read.jdbc(pg_url, table_film_category_name, properties=pg_creds)
inv_df = spark.read.jdbc(pg_url, table_inventory_name, properties=pg_creds)
rental_df = spark.read.jdbc(pg_url, table_rental_name, properties=pg_creds)
customer_df = spark.read.jdbc(pg_url, table_customer_name, properties=pg_creds)
address_df = spark.read.jdbc(pg_url, table_address_name, properties=pg_creds)
city_df = spark.read.jdbc(pg_url, table_city_name, properties=pg_creds)

rent_cities_ordered_by_rent_and_grouped_by_film_categories_df = cat_df \
    .join(film_cat_df, on="category_id") \
    .join(inv_df, on="film_id") \
    .join(rental_df, on="inventory_id") \
    .join(customer_df, on="customer_id") \
    .join(address_df, on="address_id") \
    .join(city_df, on="city_id") \
    .withColumnRenamed("name", "cat_name") \
    .withColumnRenamed("city", "city_name") \
    .withColumn("rental_diff", F.unix_timestamp(rental_df.return_date) -
                               F.unix_timestamp(rental_df.rental_date)) \
    .groupBy(cat_df.category_id \
           , "city_id" \
           , "cat_name" \
           , "city_name") \
    .agg(F.sum("rental_diff").alias("rental_diff"))

city_a_df = rent_cities_ordered_by_rent_and_grouped_by_film_categories_df \
    .filter(F.col("city_name").like("a%")) \
    .groupBy("cat_name") \
    .agg(F.sum("rental_diff").alias("rental_duration_seconds")) \
    .sort("rental_duration_seconds", ascending=False) \
    .limit(1)
    
city_dash_df = rent_cities_ordered_by_rent_and_grouped_by_film_categories_df \
    .filter(F.col("city_name").like("%-%")) \
    .groupBy("cat_name") \
    .agg(F.sum("rental_diff").alias("rental_duration_seconds")) \
    .sort("rental_duration_seconds", ascending=False) \
    .limit(1)

city_a_df.union(city_dash_df).show()

cat_df.unpersist()
film_cat_df.unpersist()
inv_df.unpersist()
rental_df.unpersist()
customer_df.unpersist()
address_df.unpersist()
city_df.unpersist()

# %%
