# pyspark-data-transformation-questions
“20 PySpark data transformation questions solved on Databricks”

# 🧩 20 PySpark Data Transformation Questions (Solved on Databricks)

This repo contains **20 PySpark Data Transformation Questions** that I solved on **Databricks**.  
Each question includes a short description and its PySpark solution.  

---

## 🧱 1. Schema & Column Operations
```python
df = spark.read.format('csv').option('Inferschema',True).option('header',True).load('/Volumes/workspace/pyspark1/big_sales/csvsource/')
```

### 🔹 Q2. Rename the column `Item_Outlet_Sales` to `Total_Sales`
```python
df.withColumnRenamed('Item_Outlet_Sales','Total_Sales').display()
```

###🔹 Q3. Drop the column Outlet_Establishment_Year
```python
df.withColumn('Outlet_Age',2009 - col('Outlet_Establishment_Year')).display()
```
🔹 Q4.Drop the column Outlet_Establishment_Year after computing Outlet_Age.
```python
df.drop('Outlet_Establishment_Year').display()
```
🔹 Q5.4.Select only the columns related to item
```python
df.select(['Item_Identifier','Item_Type','Item_MRP','Item_Weight','Item_Fat_Content']).display()
```
###🧩 2.Handling Null and Missing Values

🔹5.Find the number of null values in each column.
```python
df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).display()
```
🔹6.Replace missing Item_Weight values with the average weight of that Item_Type
```python
df_avg = df.groupBy('Item_Weight').agg(avg('Item_Weight').alias('avg_weight'))

df_join = df.join(df_avg,on='Item_Weight',how='left')

df_filled = df_join.withColumn('Item_Weight',when(col('Item_Weight').isNull(),col('avg_weight')).otherwise(col('Item_Weight'))).drop('avg_weight').display()
```
🔹7.Fill missing Outlet_Size with “Medium”.
```python
df.withColumn('Outlet_Size',when(col('Outlet_Size').isNull(),'Medium').otherwise(col('Outlet_Size'))).display()
```
###🧩3.Conditional & Derived Columns

🔹 8.Create a new column MRP_Category:“Low” if Item_MRP < 100“Medium” if 100 ≤ Item_MRP < 200“High” if Item_MRP ≥ 200**
```python
df.withColumn('MRP_Category',when(col('Item_MRP')<100,'Low')\
  .when(((col('Item_MRP')>=100) & (col('Item_MRP')<200)),"Medium")\
    .when(col('Item_MRP')>200,'High').otherwise('UNKNOWN')).display()
```
🔹 9.Create a new column Visibility_Flag:“Low Visibility” if Item_Visibility < 0.05“Normal Visibility” otherwise
```python
df.withColumn('Visibility_Flag',when(col('Item_Visibility')<0.05,'Low').otherwise('Normal')).display()
```
🔹 10.Standardize Item_Fat_Content (convert “LF”, “low fat”, “Low Fat” → “Low Fat”; “reg”, “Regular” → “Regular”).
```python
df.withColumn('Item_Fat_Content',when(col('Item_Fat_Content').isin('LF','low fat','Low Fat'),'Low Fat')\
  .when(col('Item_Fat_Content').isin('reg','regular','Regular'),'Regular').otherwise(col('Item_Fat_Content'))).display()
```
###🧩4. Aggregations & GroupBy

🔹 11.Find average Item_Outlet_Sales for each Item_Type.
```python
df.groupBy('Item_Type').agg(avg('Item_Outlet_Sales')).display()
```
🔹 12.Find total and average sales per Outlet_Type.
```python
df.groupBy('Outlet_Size').agg(avg('Item_Outlet_Sales').alias('avg_sales')).sort(col('avg_sales').desc()).display()
```
🔹 13.Find average sales per Outlet_Location_Type and sort descending
```python
df.groupBy('Outlet_Location_Type').agg(avg('Item_Outlet_Sales').alias('avg_sales')).sort(col('avg_sales').desc()).display()
```
🔹 14.Find the maximum Item_MRP for each Outlet_Identifier.
```python
df.groupBy('Outlet_Identifier').agg(max('Item_MRP')).display()
```
🔹 Q15.Compute total sales by Outlet_Type and Outlet_Location_Type together.
```python
df.groupBy('Outlet_Type','Outlet_Location_Type').agg(sum('Item_Outlet_Sales').alias('Tolat_sales')).display()
```
###🧩 5.Window, Ranking, and Filtering

🔹 For each Outlet_Identifier, rank items based on their Item_Outlet_Sales.
```python
window_spec = Window.partitionBy(col('Outlet_Identifier')).orderBy(col('Item_Outlet_Sales').desc())
df.withColumn('rank',rank().over(window_spec)).display(df)
```
🔹 Get the top 3 selling items in each outlet explain
```python
window = Window.partitionBy(col('Outlet_Identifier')).orderBy(col('Total_sales').desc())
df_result = df.groupBy('Outlet_Identifier','Item_Identifier').agg(sum('Item_Outlet_Sales').alias('Total_sales'))\
.withColumn('Rank',dense_rank().over(window))\
.filter(col('Rank')<=3).display()
```
Q3. Filter all records where Item_Visibility > 0.2 and Item_Outlet_Sales < 1000.
```python
df_filter = df.filter((col('Item_Visibility')>0.2)&(col('Item_Outlet_Sales')<1000)).display()
```
🔹Filter outlets located in “Tier 3” cities with total sales above average.
```python
from pyspark.sql.functions import sum, avg
tier3_df = df.filter(df.Outlet_Location_Type == "Tier 3")

total_sales_df = tier3_df.groupBy("Outlet_Identifier")\
.agg(sum("Item_Outlet_Sales").alias("Total_Sales"))

avg_sales = total_sales_df.agg(avg("Total_Sales").alias("Avg_Sales"))\
.collect()[0]["Avg_Sales"]

filtered_outlets = total_sales_df.filter(total_sales_df.Total_Sales > avg_sales).display()
```
###🧩6. Advanced Transformations

🔹###🧩Create a pivot table showing average Item_Outlet_Sales by Outlet_Type (rows) and Outlet_Location_Type (columns)
```python
pivot_df = (
    df.groupBy("Outlet_Type").pivot("Outlet_Location_Type")
      .agg(avg("Item_Outlet_Sales").alias("Avg_Sales"))).display()
```



















