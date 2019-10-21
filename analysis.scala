// load hive tables into dataframe
import spark.implicits._

val df0 = spark.read.table("transactions")
df0.show()

// remove header:
val header = df0.first()
val df = df0.filter(_ != header)
df.show()
df.describe().show()

// I 	City based analysis:
// Average  of  Price
df.groupBy("city").avg("price").sort(avg("price").desc).show()
+---------------+------------------+
|           city|        avg(price)|
+---------------+------------------+
|    SLOUGHHOUSE|          949000.0|
|    GRANITE BAY| 678733.3333333334|
|         WILTON|          617508.4|
|         LOOMIS|          567000.0|
|         PENRYN|          506688.0|
|EL DORADO HILLS|491698.95652173914|
|  GARDEN VALLEY|          490000.0|
|        LINCOLN|436940.59722222225|
|         FOLSOM|414960.17647058825|
|         AUBURN|          405890.8|
|      GREENWOOD|          395000.0|
|        ROCKLIN|381835.82352941175|
|   WALNUT GROVE|          380000.0|
|    PLACERVILLE|          363863.4|
|     GOLD RIVER|          358000.0|
|      ROSEVILLE|         324528.25|
|      FAIR OAKS| 303500.6666666667|
|           COOL|          300000.0|
| RANCHO MURIETA|          297750.0|
|     CARMICHAEL|         295684.75|
+---------------+------------------+
only showing top 20 rows

// Average  of square footage
df.groupBy("city").avg("area_sq_ft").sort(avg("area_sq_ft").desc).show()
+---------------+------------------+
|           city|   avg(area_sq_ft)|
+---------------+------------------+
|    SLOUGHHOUSE|            4822.0|
|         WILTON|            3487.4|
|    GRANITE BAY|            2974.0|
|      GREENWOOD|            2846.0|
|EL DORADO HILLS| 2556.695652173913|
|  GARDEN VALLEY|            2475.0|
|        ROCKLIN|2231.6470588235293|
| RANCHO MURIETA|            2225.0|
|         FOLSOM| 2200.764705882353|
|         AUBURN|            2191.4|
|         MATHER|            2093.0|
|     GOLD RIVER|           1993.25|
| RANCHO CORDOVA| 1955.642857142857|
|      ELK GROVE| 1950.517543859649|
|        LINCOLN|1915.0833333333333|
|      ROSEVILLE|1867.4166666666667|
|   WALNUT GROVE|            1727.0|
|    PLACERVILLE|            1717.9|
|       ANTELOPE|1716.6363636363637|
|      FAIR OAKS|1659.7777777777778|
+---------------+------------------+
only showing top 20 rows

// Average  of Age of the House
df.groupBy("city").avg("property_age").sort(avg("property_age").desc).show()
+---------------+------------------+
|           city| avg(property_age)|
+---------------+------------------+
|      GREENWOOD|              25.0|
|     FORESTHILL|              24.0|
|         PENRYN|              23.0|
|   MEADOW VISTA|              23.0|
|    GRANITE BAY|22.333333333333332|
|         AUBURN|              19.0|
|WEST SACRAMENTO|              19.0|
|DIAMOND SPRINGS|              19.0|
|     GOLD RIVER|             18.25|
|           COOL|              18.0|
|         LOOMIS|              17.5|
|         MATHER|              17.0|
|         FOLSOM|              16.0|
| CITRUS HEIGHTS|15.857142857142858|
|        ELVERTA|             15.75|
|   CAMERON PARK|15.555555555555555|
|      FAIR OAKS|15.333333333333334|
|     SACRAMENTO|15.287015945330296|
|    PLACERVILLE|              15.2|
|       ANTELOPE|15.030303030303031|
+---------------+------------------+
only showing top 20 rows

// Total number of transactions in each city
df.groupBy("city").count().orderBy(desc("count")).show()
+---------------+-----+
|           city|count|
+---------------+-----+
|     SACRAMENTO|  439|
|      ELK GROVE|  114|
|        LINCOLN|   72|
|      ROSEVILLE|   48|
| CITRUS HEIGHTS|   35|
|       ANTELOPE|   33|
| RANCHO CORDOVA|   28|
|EL DORADO HILLS|   23|
|NORTH HIGHLANDS|   21|
|           GALT|   21|
|     CARMICHAEL|   20|
|        ROCKLIN|   17|
|         FOLSOM|   17|
|      RIO LINDA|   13|
|     ORANGEVALE|   11|
|    PLACERVILLE|   10|
|      FAIR OAKS|    9|
|   CAMERON PARK|    9|
|         AUBURN|    5|
|         WILTON|    5|
+---------------+-----+
only showing top 20 rows

// Prime property  Type
df.groupBy("city","type").count().orderBy(desc("count")).show()
+---------------+------------+-----+
|           city|        type|count|
+---------------+------------+-----+
|     SACRAMENTO| Residential|  402|
|      ELK GROVE| Residential|  108|
|        LINCOLN| Residential|   70|
|      ROSEVILLE| Residential|   41|
|       ANTELOPE| Residential|   32|
| CITRUS HEIGHTS| Residential|   32|
|     SACRAMENTO|       Condo|   27|
| RANCHO CORDOVA| Residential|   26|
|EL DORADO HILLS| Residential|   23|
|NORTH HIGHLANDS| Residential|   21|
|           GALT| Residential|   21|
|     CARMICHAEL| Residential|   17|
|        ROCKLIN| Residential|   17|
|         FOLSOM| Residential|   16|
|      RIO LINDA| Residential|   13|
|     ORANGEVALE| Residential|   11|
|     SACRAMENTO|Multi-Family|   10|
|    PLACERVILLE| Residential|   10|
|   CAMERON PARK| Residential|    8|
|      FAIR OAKS| Residential|    8|
+---------------+------------+-----+
only showing top 20 rows

// Distinct Zip-codes  for each city
df.rollup("city","zipcode").count().orderBy("city").show(150)
+---------------+-------+-----+
|           city|zipcode|count|
+---------------+-------+-----+
|           null|   null|  985|
|       ANTELOPE|   null|   33|			
|       ANTELOPE|  95843|   33|
|         AUBURN|  95603|    5|
|         AUBURN|   null|    5|
|   CAMERON PARK|   null|    9|
|   CAMERON PARK|  95682|    9|
|     CARMICHAEL|  95608|   20|
|     CARMICHAEL|   null|   20|
| CITRUS HEIGHTS|   null|   35|			// sum of this city
| CITRUS HEIGHTS|  95621|   28|
| CITRUS HEIGHTS|  95610|    7|
|           COOL|  95614|    1|
|           COOL|   null|    1|
|DIAMOND SPRINGS|   null|    1|
|DIAMOND SPRINGS|  95619|    1|
|      EL DORADO|   null|    2|
|      EL DORADO|  95623|    2|
|EL DORADO HILLS|  95762|   23|
|EL DORADO HILLS|   null|   23|
|      ELK GROVE|  95758|   44|
|      ELK GROVE|  95624|   34|
|      ELK GROVE|  95757|   36|
|      ELK GROVE|   null|  114|
|        ELVERTA|   null|    4|
|        ELVERTA|  95626|    4|
|      FAIR OAKS|   null|    9|
|      FAIR OAKS|  95628|    9|
|         FOLSOM|  95630|   17|
|         FOLSOM|   null|   17|
|     FORESTHILL|  95631|    1|
|     FORESTHILL|   null|    1|
|           GALT|   null|   21|
|           GALT|  95632|   21|
|  GARDEN VALLEY|   null|    1|
|  GARDEN VALLEY|  95633|    1|
|     GOLD RIVER|  95670|    4|
|     GOLD RIVER|   null|    4|
|    GRANITE BAY|   null|    3|
|    GRANITE BAY|  95746|    3|
|      GREENWOOD|  95635|    1|
|      GREENWOOD|   null|    1|
|        LINCOLN|   null|   72|
|        LINCOLN|  95648|   72|
|         LOOMIS|  95650|    2|
|         LOOMIS|   null|    2|
|         MATHER|  95655|    1|
|         MATHER|   null|    1|
|   MEADOW VISTA|  95722|    1|
|   MEADOW VISTA|   null|    1|
|NORTH HIGHLANDS|   null|   21|
|NORTH HIGHLANDS|  95660|   21|
|     ORANGEVALE|   null|   11|
|     ORANGEVALE|  95662|   11|
|         PENRYN|   null|    1|
|         PENRYN|  95663|    1|
|    PLACERVILLE|  95667|   10|
|    PLACERVILLE|   null|   10|
|  POLLOCK PINES|   null|    3|
|  POLLOCK PINES|  95726|    3|
| RANCHO CORDOVA|  95670|   17|
| RANCHO CORDOVA|   null|   28|
| RANCHO CORDOVA|  95742|   11|
| RANCHO MURIETA|  95683|    3|
| RANCHO MURIETA|   null|    3|
|      RIO LINDA|  95673|   13|
|      RIO LINDA|   null|   13|
|        ROCKLIN|   null|   17|
|        ROCKLIN|  95765|   11|
|        ROCKLIN|  95677|    6|
|      ROSEVILLE|  95678|   20|
|      ROSEVILLE|  95661|    8|
|      ROSEVILLE|   null|   48|
|      ROSEVILLE|  95747|   20|
|     SACRAMENTO|  95822|   24|
|     SACRAMENTO|  95826|   18|
|     SACRAMENTO|  95819|    4|
|     SACRAMENTO|  95841|    7|
|     SACRAMENTO|  95833|   20|
|     SACRAMENTO|  95824|   12|
|     SACRAMENTO|  95864|    5|
|     SACRAMENTO|  95817|    7|
|     SACRAMENTO|  95831|   10|
|     SACRAMENTO|  95838|   37|
|     SACRAMENTO|  95827|    9|
|     SACRAMENTO|  95825|   13|
|     SACRAMENTO|  95821|    6|
|     SACRAMENTO|  95829|   11|
|     SACRAMENTO|  95832|   12|
|     SACRAMENTO|  95835|   37|
|     SACRAMENTO|  95820|   23|
|     SACRAMENTO|  95823|   61|
|     SACRAMENTO|  95834|   22|
|     SACRAMENTO|   null|  439|
|     SACRAMENTO|  95811|    2|
|     SACRAMENTO|  95842|   22|
|     SACRAMENTO|  95828|   45|
|     SACRAMENTO|  95818|    7|
|     SACRAMENTO|  95816|    4|
|     SACRAMENTO|  95815|   18|
|     SACRAMENTO|  95814|    3|
|SHINGLE SPRINGS|  95682|    1|
|SHINGLE SPRINGS|   null|    1|
|    SLOUGHHOUSE|  95683|    1|
|    SLOUGHHOUSE|   null|    1|
|   WALNUT GROVE|  95690|    1|
|   WALNUT GROVE|   null|    1|
|WEST SACRAMENTO|   null|    3|
|WEST SACRAMENTO|  95691|    3|
|         WILTON|  95693|    5|
|         WILTON|   null|    5|
+---------------+-------+-----+


// II 	Which type of property is selling well
// depending on beds and baths
df.groupBy("bedrooms","baths").count().orderBy(desc("count"))show()
+--------+-----+-----+
|bedrooms|baths|count|
+--------+-----+-----+
|       3|    2|  317|
|       4|    2|  178|
|       3|    1|   90|
|       4|    3|   88|
|       2|    1|   78|
|       2|    2|   59|
|       5|    3|   48|
|       5|    4|   46|
|       4|    4|   29|
|       3|    3|   23|
|       1|    1|   10|
|       5|    2|    6|
|       4|    1|    3|
|       6|    4|    3|
|       5|    5|    2|
|       2|    3|    1|
|       6|    5|    1|
|       3|    4|    1|
|       6|    3|    1|
|       8|    4|    1|
+--------+-----+-----+

//depending on area(sqft) range

import org.apache.spark.ml.feature.Bucketizer

val splits = Range.Double(0,6000,1000).toArray

val bucketizer = new Bucketizer().setInputCol("area_sq_ft").setOutputCol("area_sqft_range").setSplits(splits)

val df2 = bucketizer.transform(df)

df2.groupBy("area_sqft_range").count().orderBy(desc("count")).show()

// 0.0: 0-1000; 1.0: 1000-2000; 2.0: 2000-3000; 3.0: 3000-4000; 4.0: 4000-5000
+---------------+-----+
|area_sqft_range|count|
+---------------+-----+
|            1.0|  667|
|            2.0|  147|
|            0.0|  109|
|            3.0|   57|
|            4.0|    5|
+---------------+-----+


// III 	Positive/Negative relationship 
// between Rate  and Price
// very weak positive relationship 
df.stat.corr("locality_rating", "price")
res88: Double = 0.04623665207343453

df.stat.corr("locality_rating", "price_sq_ft")
res89: Double = 0.006328063486050017

// IV Which property type falls into a particular group(Locality_rating, age group)
// 1. Groups of rating 
import org.apache.spark.ml.feature.Bucketizer

val splits = Range.Double(0,10,3).toArray

val bucketizer = new Bucketizer().setInputCol("locality_rating").setOutputCol("rating_group").setSplits(splits)

val df2 = bucketizer.transform(df)

df2.groupBy("rating_group").count().orderBy(desc("count")).show()
// 0.0: 0-2; 1.0: 3-5; 2.0: 6-9
+------------+-----+
|rating_group|count|
+------------+-----+
|         2.0|  472|
|         1.0|  372|
|         0.0|  141|
+------------+-----+

// 2. Age groups
import org.apache.spark.ml.feature.Bucketizer

val splits = Range.Double(5,26,5).toArray

val bucketizer = new Bucketizer().setInputCol("property_age").setOutputCol("age_group").setSplits(splits)

val df2 = bucketizer.transform(df)

df2.groupBy("age_group").count().orderBy(desc("count")).show()
//0.0: 5-9; 1.0: 10-14; 2.0: 15-19; 3.0: 20-25
+---------+-----+
|age_group|count|
+---------+-----+
|      3.0|  292|
|      1.0|  245|
|      0.0|  234|
|      2.0|  214|
+---------+-----+

// V top 10
// most expensive house, city, bedrooms, baths
df.groupBy("city","bedrooms","baths").max("price").orderBy(max("price").desc).show(10)
+---------------+--------+-----+----------+
|           city|bedrooms|baths|max(price)|
+---------------+--------+-----+----------+
|    SLOUGHHOUSE|       3|    4|    949000|		<---
|         WILTON|       4|    3|    884790|
|EL DORADO HILLS|       4|    3|    879000|
|         LOOMIS|       4|    4|    839000|
|EL DORADO HILLS|       6|    5|    830000|
|    GRANITE BAY|       5|    3|    760000|
|        LINCOLN|       3|    3|    755100|
|     SACRAMENTO|       5|    2|    699000|
|         WILTON|       5|    3|    691659|
|EL DORADO HILLS|       5|    5|    680000|
+---------------+--------+-----+----------+

// the cheapest house, city, bedrooms, baths
df.groupBy("city","bedrooms","baths").max("price").orderBy(max("price")).show(10)
+---------------+--------+-----+----------+
|           city|bedrooms|baths|max(price)|
+---------------+--------+-----+----------+
|NORTH HIGHLANDS|       2|    1|     63000|		<---
|      ELK GROVE|       2|    1|     71000|
| RANCHO MURIETA|       2|    2|     97750|
|      ELK GROVE|       1|    1|    100000|
| RANCHO CORDOVA|       2|    1|    115000|
| CITRUS HEIGHTS|       2|    1|    116250|
|        ELVERTA|       4|    2|    126714|
|NORTH HIGHLANDS|       2|    2|    131750|
|      RIO LINDA|       2|    1|    132000|
| RANCHO CORDOVA|       3|    1|    134000|
+---------------+--------+-----+----------+