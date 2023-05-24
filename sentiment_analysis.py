import json
import requests
from bs4 import BeautifulSoup
import psycopg2
import boto3
import pandas as pd
from datetime import datetime
from botocore.exceptions import ClientError


def get_secret():
    secret_name = "redshift-conn"
    region_name = "us-east-1"
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    secret = json.loads(get_secret_value_response['SecretString'])
    return secret

secret = get_secret()
db_host = secret['host']
db_port = secret['port']
db_name = secret['database']
db_user = secret['user']
db_password = secret['password']

comprehend = boto3.client("comprehend", region_name= 'ap-south-1')
conn = psycopg2.connect(host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password)

# to get the page source i.e. html body of url
def get_soup(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    return soup

# to find the sentiment of article 
def get_sentiment(article):
    lst = []
    # aws_comprehend has limit of 5000 bytes --> while loop makes partion of body < 4500 bytes --> works until body is > 4500bytes
    while len(article) > 4500:
        detect = comprehend.detect_sentiment(Text = article[:4500], LanguageCode= "en")
        article = article[4500:]
        lst.append(detect)
    # works when body is < 4500 bytes or when body is reduced to < 4500 bytes (hence always)
    detect = comprehend.detect_sentiment(Text = article, LanguageCode= "en")
    lst.append(detect)
    SentimentScore = {'Positive': 0, 'Negative': 0, 'Neutral': 0, 'Mixed': 0}
    for i in lst:
        SentimentScore["Positive"] += i["SentimentScore"]["Positive"]
        SentimentScore["Negative"] += i["SentimentScore"]["Negative"]
        SentimentScore["Neutral"] += i["SentimentScore"]["Neutral"]
        SentimentScore["Mixed"] += i["SentimentScore"]["Mixed"]
    # gives sentiment which has maximum value    
    sentiment = max(SentimentScore, key = SentimentScore.get)
    return sentiment


def sentiment_analysis():
    cursor = conn.cursor()
    # Fetching data from database table_1 
    query = 'select * from table_1 where Active_Flag=1'
    cursor.execute(query)
    rows = cursor.fetchall()
    
    # Creating respective tables
    cursor.execute("CREATE TABLE IF NOT EXISTS table_2 ( News_URL varchar(255), RSS_Feed varchar(255), Active_Flag int, Updated_Time_Stamp DATETIME)")
    cursor.execute("""CREATE TABLE IF NOT EXISTS table_3 ( RSS_Feed varchar(255),
                                                        Title varchar(1000),
                                                        Description varchar(5000),
                                                        Body varchar(30000),
                                                        Publish_Date varchar(255),
                                                        Inserted_Time_Stamp DATETIME,
                                                        Sentiment varchar(255),
                                                        RSS_Feed_URL varchar(255)
                                                        )""")        
    conn.commit()
    df=pd.DataFrame(columns=["News_Origin","News_URL","Active_Flag"])
    for i in rows:    # Putting fetched values in DataFrame
        df.loc[len(df)]=i
    print(df)
    result_df_1 = pd.DataFrame(columns=['News_URL', 'RSS_Feed', 'Active_Flag', 'Updated_Time_Stamp'])  # define first dataframe
    for index, row in df.iterrows():   # iterate through filtered dataframe
        news_url = row['News_URL']
        rss_feed = ''
        soup_1 = get_soup(news_url)
        all_links = soup_1.find_all('a')  # find all links present in url
        rss_feed_link = None

        for link in all_links:
            if 'rss' in link.get('href', '') or 'rss' in link.text.lower():  # filter links for rss_feed_link
                rss_feed_link = link
                break
        if rss_feed_link is not None:
            rss_feed = rss_feed_link['href']
        else:
            print('RSS link not found.')

        # appending data to dataframe
        result_df_1 = result_df_1.append({'News_URL': news_url, 'Active_Flag': row['Active_Flag'], 'RSS_Feed': rss_feed,
                                          'Updated_Time_Stamp': datetime.now()}, ignore_index=True)
        
        # Inserting values in database table_2
        for index, row in result_df_1.iterrows():
            cursor.execute("INSERT INTO table_2 VALUES (%s, %s, %s, %s)", 
                          ( row['News_URL'], row['RSS_Feed'], row['Active_Flag'], row['Updated_Time_Stamp']))
        conn.commit()
        
        for i, r in result_df_1.iterrows():  # iterate in first formed dataframe
            rss_feed = r["RSS_Feed"]
            news_url = r["News_URL"][8:]
            soup_2 = get_soup(rss_feed)
            href_list = []
            for link in soup_2.find_all('a'):  # find for the rss_feed link
                href = link.get('href')
                # if href and "rssfeedstopstories" in href:
                if href and href.endswith(".cms") and(href.startswith(f"https://{news_url}rssfeed") or href.startswith(f"http://{news_url}rssfeed")) and "video" not in href:
                    href_list.append(href)
            # link = href_list[0]
            for link in list(set(href_list)):   # iterate through the list of all rss feed links
                soup_3 = get_soup(link)
                rss_feed_url = link
                items = soup_3.find_all("item")
    
                for item in items:     # to find individual items data
                    title = str(item.title.text).replace("'", "‘")
                    description = item.description.text.split('</a>')[-1].strip()
                    if len(description) == 0:
                        description = title.strip()
                    body = item.guid.text
                    try:
                        publish_date = item.pubdate.text
                    except:
                        publish_date = item.pubDate.text

                    # comparing data with database
                    cursor = conn.cursor()
                    query = f"select * from table_3 where title = '{title}' "
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    if not rows:
                        try:
                            # to find the body from the article url
                            soup_4 = get_soup(body)
                            script_tag = soup_4.find_all("script", {"type": "application/ld+json"})
                            article = ""
                            for script in script_tag:
                                json_obj = json.loads(script.string)
                                if "articleBody" in json_obj.keys():
                                    article = json_obj['articleBody']
                        except:
                            continue
                        body = article
                        print(len(body))
                        if len(body) > 0:
                            sentiment = get_sentiment(article)
                            print(sentiment)
                            # inserting values in database table_3
                            cursor.execute("""
                                INSERT INTO table_3 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """, ( rss_feed, title, str(description).replace("'", "‘"), str(body).replace("'", "‘"),
                                  publish_date, datetime.now(), sentiment, rss_feed_url ))
    
                            conn.commit()


sentiment_analysis()
