Data bisa di-extract dari berbagai sumber. Berikut sumber-sumber umum beserta contoh code-nya.

# 1. Database Relational (MySQL, PostgreSQL, SQL Server)
Sumber Data:

Database transaksi penjualan
Database pelanggan (CRM)
Database inventory
Database HR/karyawan

## Contoh Code Extract dari PostgreSQL:

    from airflow.decorators import task
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    @task()
    def extract_from_postgres():
        """Extract data penjualan dari PostgreSQL"""
        pg_hook = PostgresHook(postgres_conn_id='postgres_sales')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
    
    # Query data penjualan hari ini
    query = """
    SELECT order_id, customer_id, product_id, quantity, price, order_date
    FROM sales_transactions
    WHERE order_date = CURRENT_DATE
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    # Convert ke list of dictionaries
    columns = [desc[0] for desc in cursor.description]
    data = [dict(zip(columns, row)) for row in results]
    
    cursor.close()
    return data

## Contoh Extract dari MySQL:
    from airflow.providers.mysql.hooks.mysql import MySqlHook
    
    @task()
    def extract_from_mysql():
        """Extract data customer dari MySQL"""
        mysql_hook = MySqlHook(mysql_conn_id='mysql_crm')
        
    query = """
    SELECT customer_id, name, email, phone, registration_date
    FROM customers
    WHERE status = 'active'
    """
    
    # Langsung return sebagai pandas DataFrame
    df = mysql_hook.get_pandas_df(query)
    return df.to_dict('records')

# 2. REST API (Third-Party Services)
Sumber Data:

API payment gateway (Stripe, Midtrans)
API marketplace (Tokopedia, Shopee, Amazon)
API sosial media (Facebook Ads, Instagram)
API analytics (Google Analytics)
API logistics (JNE, SiCepat)

## Contoh Extract dari REST API:
    from airflow.providers.http.hooks.http import HttpHook
    import json
    
    @task()
    def extract_from_stripe_api():
        """Extract data payment dari Stripe"""
        http_hook = HttpHook(http_conn_id='stripe_api', method='GET')
    
    # Get transactions from last 24 hours
    endpoint = '/v1/charges?limit=100'
    
    response = http_hook.run(endpoint)
    
    if response.status_code == 200:
        data = response.json()
        return data['data']  # List of charges
    else:
        raise Exception(f"Failed to fetch Stripe data: {response.status_code}")

    @task()
    def extract_from_tokopedia_api():
        """Extract data pesanan dari Tokopedia"""
        import requests
    
    # Biasanya perlu authentication token
    headers = {
        'Authorization': 'Bearer YOUR_TOKEN',
        'Content-Type': 'application/json'
    }
    
    url = 'https://fs.tokopedia.net/v2/order/list'
    params = {
        'shop_id': '12345',
        'from_date': '2024-01-01',
        'to_date': '2024-01-31'
    }
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        return response.json()['data']
    else:
        raise Exception(f"Failed to fetch Tokopedia orders: {response.status_code}")

# 3. File Storage (CSV, Excel, JSON)
Sumber Data:

File laporan dari vendor
Export data dari sistem legacy
File dari FTP/SFTP server
File dari cloud storage (S3, Google Drive)

## Contoh Extract dari CSV di Local/Server:
    import pandas as pd
    
    @task()
    def extract_from_csv():
        """Extract data dari CSV file"""
        file_path = '/data/sales_report_2024.csv'
    
    df = pd.read_csv(file_path)
    return df.to_dict('records')

## Contoh Extract dari Excel:
    @task()
    def extract_from_excel():
        """Extract data dari Excel file"""
        file_path = '/data/inventory_report.xlsx'
    
    # Baca specific sheet
    df = pd.read_excel(file_path, sheet_name='January')
    return df.to_dict('records')

## Contoh Extract dari Amazon S3:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    import pandas as pd
    from io import StringIO
    
    @task()
    def extract_from_s3():
        """Extract CSV dari Amazon S3"""
        s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # Download file dari S3
    file_content = s3_hook.read_key(
        key='sales-data/daily_sales.csv',
        bucket_name='my-company-bucket'
    )
    
    # Convert to DataFrame
    df = pd.read_csv(StringIO(file_content))
    return df.to_dict('records')

## Contoh Extract dari Google Drive:
    from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
    
    @task()
    def extract_from_google_drive():
        """Extract file dari Google Drive"""
        drive_hook = GoogleDriveHook(gcp_conn_id='google_cloud_default')
    
    # Download file by ID
    file_id = '1ABC...XYZ'
    file_content = drive_hook.download_file(file_id=file_id)
    
    # Process file (misalnya CSV)
    df = pd.read_csv(StringIO(file_content.decode('utf-8')))
    return df.to_dict('records')

# 4. Cloud Data Warehouse
Sumber Data:

BigQuery (Google)
Redshift (Amazon)
Snowflake
Azure Synapse

## Contoh Extract dari Google BigQuery:
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    
    @task()
    def extract_from_bigquery():
        """Extract data dari Google BigQuery"""
        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
    
    query = """
    SELECT 
        date,
        product_id,
        SUM(revenue) as total_revenue,
        COUNT(order_id) as total_orders
    FROM `project.dataset.sales`
    WHERE date = CURRENT_DATE()
    GROUP BY date, product_id
    """
    
    df = bq_hook.get_pandas_df(sql=query)
    return df.to_dict('records')

## Contoh Extract dari Snowflake:
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
    @task()
    def extract_from_snowflake():
        """Extract data dari Snowflake"""
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    query = """
    SELECT customer_id, total_purchases, last_purchase_date
    FROM ANALYTICS.CUSTOMERS
    WHERE status = 'ACTIVE'
    """
    
    df = snowflake_hook.get_pandas_df(query)
    return df.to_dict('records')

# 5. NoSQL Database (MongoDB, Cassandra)
Sumber Data:

Database log aplikasi
Database user behavior
Database real-time data

## Contoh Extract dari MongoDB:
    from airflow.providers.mongo.hooks.mongo import MongoHook
    
    @task()
    def extract_from_mongodb():
        """Extract data dari MongoDB"""
        mongo_hook = MongoHook(conn_id='mongo_default')
        client = mongo_hook.get_conn()
    
    db = client['ecommerce']
    collection = db['user_activities']
    
    # Query data
    query = {
        'event_date': {'$gte': '2024-01-01'},
        'event_type': 'purchase'
    }
    
    results = list(collection.find(query))
    
    # Convert ObjectId to string
    for doc in results:
        doc['_id'] = str(doc['_id'])
    
    return results

# 6. FTP/SFTP Server
Sumber Data:

File dari partner bisnis
File dari vendor eksternal
Automated reports

## Contoh Extract dari SFTP:
    from airflow.providers.sftp.hooks.sftp import SFTPHook
    import pandas as pd

    @task()
    def extract_from_sftp():
        """Extract CSV dari SFTP server"""
        sftp_hook = SFTPHook(ssh_conn_id='sftp_vendor')
    
    remote_path = '/reports/daily_sales.csv'
    local_path = '/tmp/daily_sales.csv'
    
    # Download file
    sftp_hook.retrieve_file(remote_path, local_path)
    
    # Read CSV
    df = pd.read_csv(local_path)
    return df.to_dict('records')

# 7. Web Scraping
Sumber Data:

Data kompetitor dari website
Data harga pasar
News/artikel

## Contoh Web Scraping:
    from bs4 import BeautifulSoup
    import requests
    
    @task()
    def extract_from_website():
        """Scrape data harga produk kompetitor"""
        url = 'https://competitor.com/products'
    
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    products = []
    for item in soup.find_all('div', class_='product-item'):
        product = {
            'name': item.find('h3').text,
            'price': item.find('span', class_='price').text,
            'stock': item.find('span', class_='stock').text
        }
        products.append(product)
    
    return products

# 8. Streaming Data (Kafka, Kinesis)
Sumber Data:

Real-time IoT sensor data
Live transaction data
User clickstream data

## Contoh Extract dari Kafka:
    from kafka import KafkaConsumer
    import json
    
    @task()
    def extract_from_kafka():
        """Extract messages dari Kafka topic"""
        consumer = KafkaConsumer(
            'transactions-topic',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
    
    messages = []
    for message in consumer:
        messages.append(message.value)
        
        # Stop after 1000 messages atau timeout
        if len(messages) >= 1000:
            break
    
    consumer.close()
    return messages

# Contoh pipeline lengkap dengan multiple sources

    from airflow import DAG
    from airflow.decorators import task
    from airflow.utils.dates import days_ago
    from datetime import timedelta
    
    default_args = {
        'owner': 'data_team',
        'start_date': days_ago(1),
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
    }
    
    with DAG(
        dag_id='multi_source_etl_pipeline',
        default_args=default_args,
        schedule_interval='@hourly',
        catchup=False
    ) as dag:
    
    # Extract dari berbagai sumber
    @task()
    def extract_sales_from_postgres():
        # Code extract dari PostgreSQL
        pass
    
    @task()
    def extract_orders_from_api():
        # Code extract dari REST API
        pass
    
    @task()
    def extract_inventory_from_csv():
        # Code extract dari CSV
        pass
    
    @task()
    def combine_all_data(sales_data, api_data, csv_data):
        """Gabungkan semua data dari berbagai sumber"""
        combined = {
            'sales': sales_data,
            'orders': api_data,
            'inventory': csv_data
        }
        return combined
    
    @task()
    def transform_data(combined_data):
        # Transform logic
        pass
    
    @task()
    def load_to_warehouse(transformed_data):
        # Load ke data warehouse
        pass
    
    # Define workflow
    sales = extract_sales_from_postgres()
    orders = extract_orders_from_api()
    inventory = extract_inventory_from_csv()
    
    combined = combine_all_data(sales, orders, inventory)
    transformed = transform_data(combined)
    load_to_warehouse(transformed)
