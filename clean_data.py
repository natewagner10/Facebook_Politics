
#https://stackoverflow.com/questions/40327859/pyspark-how-to-read-csv-into-dataframe-and-manipulate-it

# load in data
#political_ads = sc.textFile("/usr/data/facebook/fbpac-ads-en-US.csv")                     
politic = sc.textFile.sample(False, .0001, 12345)
                          
import pyspark
from csv import reader
import pandas as pd
sc = pyspark.SparkContext()
sql = SQLContext(sc)

df = (sql.read
         .format("com.databricks.spark.csv")
         .option("header", "true")
         .load("/home/amanda/fbpac-ads-en-US.csv"))
df1 = df.sample(False, 0.1, seed=7).limit(1)     
         
#df.show(2)
df2 = df.drop('html')





#// these lines are equivalent in Spark 2.0 - using [SparkSession][1]
from pyspark.sql import SparkSession

sc = pyspark.SparkContext("local[*]", "facebook")

spark.read.format("csv").option("header", "true").load("/home/amanda/fbpac-ads-en-US.csv") 
spark.read.option("header", "true").csv("/home/amanda/fbpac-ads-en-US.csv")                          



data_remove_header = political_ads.filter(lambda x: x!= 'id,html,political,not_political,title,message,thumbnail,created_at,updated_at,lang,images,impressions,political_probability,targeting,suppressed,targets,advertiser,entities,page,lower_page,targetings,paid_for_by,targetedness,listbuilding_fundraising_proba')

#split data
def string_split(line):
    return line.split(",")
    
political_ads_more = data_remove_header.map(string_split)


# remove " " from data
def clean_strings(line):
    return (line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7], line[8], line[9], line[10], line[11], line[12], line[13], line[14], line[15], line[16], line[17], line[18], line[19], line[20], line[21], line[22], line[23], line[24], line[25], line[26], line[27], line[28])

political_ads_header = political_ads_more.map(clean_strings)


    

# take out url in html column
def pull_out_url(line):
    for content in line[1]:
        content = re.findall('https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', content)        
        for url in content:
            facebookurl = "https://www.facebook.com"
            pp_image_url = "https://pp-facebook-ads.s3.amazonaws.com"
            bitly_url = "https://bit.ly"
            if facebookurl == url:
                content.remove(url)
            elif pp_image_url == url:
                content.remove(url)
            elif bitly_url == url:
                content.remove(url)
            else:
                return url
                #company_url = url
                #return (line[0], company_url, line[2])

political_ads = political_ads.map(pull_out_url)

# remove headers
politic_ad_data = political_ads.filter(lambda x: x!= '"Date/Time","Lat","Lon","Base"')

