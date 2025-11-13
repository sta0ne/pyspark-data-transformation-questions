# pyspark-data-transformation-questions
“20 PySpark data transformation questions solved on Databricks”

# 🧩 20 PySpark Data Transformation Questions (Solved on Databricks)

This repo contains **20 PySpark Data Transformation Questions** that I solved on **Databricks**.  
Each question includes a short description and its PySpark solution.  

---

## 🧱 1. Schema & Column Operations

### 🧩 Q1. Rename the column `Item_Outlet_Sales` to `Total_Sales`
```python
df = spark.read.format('csv').option('Inferschema',True).option('header',True).load('/Volumes/workspace/pyspark1/big_sales/csvsource/')'''

#🧩 Q2. Add a new column Outlet_Age = (2025 - Outlet_Establishment_Year)
from pyspark.sql.functions import col

df = df.withColumn("Outlet_Age", 2025 - col("Outlet_Establishment_Year"))

