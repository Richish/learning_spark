#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import count


# In[2]:


spark = SparkSession.builder.appName("PythonMnMCount").getOrCreate()


# In[3]:


mnm_file = "data/mnm_dataset.csv"
mnm_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(mnm_file)


# In[4]:


dir(mnm_df)


# In[5]:


#mnm_df.show(2)
count_mnm_df = mnm_df.select("State", "Color", "Count").groupBy("State", "Color").agg(count("Count").alias("Total")).orderBy("Total", ascending=False)
count_mnm_df.show(60, truncate=False)


# In[6]:


print("Total Rows = %d" % (count_mnm_df.count()))


# In[7]:


ca_count_mnm_df = (mnm_df
.select("State", "Color", "Count")
.where(mnm_df.State == "CA")
.groupBy("State", "Color")
.agg(count("Count").alias("Total"))
.orderBy("Total", ascending=False))
# Show the resulting aggregation for California.
ca_count_mnm_df.show(n=10, truncate=False)


# In[8]:


spark.stop()


# In[9]:


#exit()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




