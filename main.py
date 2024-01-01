import pandas as pd
import datetime as dt
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.float_format', lambda x: '%.2f' % x)
pd.set_option('display.width',1000)

#Reading Dataset
df_=pd.read_csv("flo_data_20k.csv")
df=df_.copy()
df.head(10)
df.columns
df.shape
df.describe().T
df.isnull().sum()
df.info()

#Omnichannel means that customers shop both online and offline platforms. Total for each customer.Create new variables for the number of purchases and spending
df["order_num_total"]=df["order_num_total_ever_online"]+df["order_num_total_ever_offline"]
df["customer_value_total"]=df["customer_value_total_ever_offline"]+df["customer_value_total_ever_online"]

#Examine the variable types. Change the type of the variables that give the historical expression to date.
date_columns = df.columns[df.columns.str.contains("date")]
df[date_columns]=df[date_columns].apply(pd.to_datetime)

#Show the distribution of the number of customers, total number of products purchased and total expenditures in shopping channels
df.groupby("order_channel").agg({"master_id":"count",
                                 "order_num_total":"sum",
                                 "customer_value_total":"sum"})

#Rank the top 10 most profitable customers
df.sort_values(by="customer_value_total",ascending=False).head(10)

#List the top 10 customers who place the most orders.
df.sort_values(by="order_num_total",ascending=False)[:10]

#Functionalize the data preparation process
def data_prep(dataframe):
    dataframe["order_num_total"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
    dataframe["customer_value_total"] = dataframe["customer_value_total_ever_offline"] + dataframe["customer_value_total_ever_online"]
    date_columns = dataframe.columns[dataframe.columns.str.contains("date")]
    dataframe[date_columns] = dataframe[date_columns].apply(pd.to_datetime)
    dataframe.groupby("order_channel").agg({"master_id": "count",
                                     "order_num_total": "sum",
                                     "customer_value_total": "sum"})

    dataframe.sort_values(by="customer_value_total", ascending=False).head(10)
    dataframe.sort_values(by="order_num_total", ascending=False)[:10]
    return dataframe

#Calculation of RFM Metrics
#The analysis date is 2 days after the date of the last purchase in the data set.
df["last_order_date"].max()
analysis_date =dt.datetime(2021,6,1)

rfm=pd.DataFrame()
rfm["customer_id"]=df["master_id"]
rfm["recency"] = (analysis_date - df["last_order_date"]).values.astype('timedelta64[D]')
rfm["frequency"]=df["order_num_total"]
rfm["monetary"]=df["customer_value_total"]

#Calculation of RF Score
rfm["recency_score"]=pd.qcut(rfm["recency"],5,labels=[5,4,3,2,1])
rfm["frequency_score"]=pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
rfm["monetary_score"]=pd.qcut(rfm["monetary"],5,labels=[1,2,3,4,5])

rfm["RF_SCORE"]=(rfm["recency_score"].astype(str)+rfm["frequency_score"].astype(str))

#RFM Segmentation
seg_map = {
    r'[1-2][1-2]':'hibernating',
    r'[1-2][3-4]':'at_Risk',
    r'[1-2]5':'cant_loose',
    r'3[1-2]':'about_to_sleep',
    r'33':'need_attention',
    r'[3-4][4-5]':'loyal_customers',
    r'41':'promising',
    r'51':'new_customers',
    r'[4-5][2-3]':'potential_loyalists',
    r'5[4-5]':'champions'
}

rfm['segment']=rfm['RF_SCORE'].replace(seg_map,regex=True)

#Examine the recency, frequency and monetary averages of the segments
rfm.groupby("segment").agg({"recency":("mean","count"),
                           "frequency":("mean","count"),
                           "monetary":("mean","count")})


#PROBLEM-1
#FLO is adding a new women's shoe brand. The product prices of the included brand are available to the general customer.above your preferences. For this reason, we specifically work with customers who will be interested in the promotion of the brand and product sales.You want to contact. Shopping from loyal customers (champions, loyal_customers) and women category Customers who will be contacted specifically. Save the ID numbers of these customers in the csv file.
target_customer_id =rfm[rfm["segment"].isin(["champions","loyal_customers"])]["customer_id"]
cust_id=df[(df["master_id"].isin(target_customer_id))&(df["interested_in_categories_12"].str.contains("KADIN"))]["master_id"]
cust_id.to_csv("yeni_marka_hedef_müsteri_id.csv", index=False)

#PROBLEM-2
#Nearly 40% discount is planned for Men's and Children's products. In the past interested in categories related to this discount customers who are good customers but have not shopped for a long time and should not be lost, those who are dormant and new Incoming customers are wanted to be specifically targeted. Save the ids of the customers with the appropriate profile in the csv file.

target_customer_id=rfm[rfm["segment"].isin(["cant_loose","hibernating","new_customers"])]["customer_id"]
cust_id=df[(df["master_id"].isin(target_customer_id))&(df["interested_in_categories_12"].str.contains("ERKEK"))|(df["interested_in_categories_12"].str.contains("COCUK"))]["master_id"]
cust_id.to_csv("indirim_hedef_müşteri_ids.csv", index=False)