import os
import sqlite3
from datetime import datetime
from clickhouse_driver import Client

def get_clickhouse_client():
    """获取ClickHouse客户端连接"""
    import os
    from clickhouse_driver import Client
    from clickhouse_driver.errors import Error as ClickhouseError
    from retrying import retry

    @retry(stop_max_attempt_number=3, wait_fixed=2000)
    def create_client():
        try:
            return Client(
                host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
                port=int(os.getenv('CLICKHOUSE_PORT', 9000)),
                user='default',
                password='123456',
                secure=bool(os.getenv('CLICKHOUSE_SECURE', False)),
                verify=bool(os.getenv('CLICKHOUSE_VERIFY_SSL', True)),
                connect_timeout=10,
                send_receive_timeout=300,
                compression=True
            )
        except ClickhouseError as e:
            print(f"ClickHouse connection error: {e}")
            raise

    return create_client()

def test_clickhouse_connection():
    try:
        client = get_clickhouse_client()
        # Test connection by listing databases
        databases = client.execute('SHOW DATABASES')
        print("Successfully connected to ClickHouse!")
        print("Available databases:")
        for db in databases:
            print(f"- {db[0]}")
            
        # Create a test table
        client.execute('CREATE DATABASE IF NOT EXISTS test_db')
        client.execute('''
            CREATE TABLE IF NOT EXISTS test_db.test_table (
                id UInt32,
                name String
            ) ENGINE = MergeTree()
            ORDER BY id
        ''')
        
        # Insert test data
        client.execute('INSERT INTO test_db.test_table (id, name) VALUES', [(1, 'Alice'), (2, 'Bob')])
        
        # Query test data
        result = client.execute('SELECT * FROM test_db.test_table ORDER BY id')
        print("\nTest table data:")
        for row in result:
            print(f"ID: {row[0]}, Name: {row[1]}")

    except ConnectionError as e:
        print(f"Connection failed: {e}")
        print("Please check:")
        print("1. ClickHouse server is running")
        print("2. Network connectivity between host and container")
        print("3. Firewall settings")
    except TimeoutError as e:
        print(f"Connection timeout: {e}")
        print("Please check:")
        print("1. ClickHouse server load")
        print("2. Network latency")
    except Exception as e:
        print(f"Unexpected error: {e}")
        print("Error details:")
        print(f"Type: {type(e)}")
        print(f"Args: {e.args}")

# 表结构定义
TABLE_SCHEMA = {
    'brandOriginalId': 'UInt64',
    'reportDate': 'Date',
    'date': 'Date',
    'year': 'UInt16',
    'month': 'UInt8',
    'week': 'UInt32',
    'quarter': 'UInt8',
    'updateTimestamp': 'DateTime',
    'updatedBy': 'String',
    'spBrandedSearchPct': 'Float64',
    'sbBrandedPct': 'Float64',
    'spNtbPct': 'Float64',
    'dspNtbPct': 'Float64',
    'glanceViewPct': 'Float64',
    'ctr': 'Float64',
    'discount': 'Nullable(Float64)',
    'keywordSimilarity': 'Nullable(Float64)',
    'sovWeighted': 'Nullable(Float64)',
    'organicRanking': 'Nullable(Float64)',
    'amazonBrandRanking': 'Nullable(Float64)',
    'repeatedPurchase': 'Float64',
    'spendWeight': 'Float64',
    'spBrandedSearchPctWeight': 'Float64',
    'sbBrandedPctWeight': 'Float64',
    'spNtbPctWeight': 'Float64',
    'dspNtbPctWeight': 'Float64',
    'glanceViewPctWeight': 'Float64',
    'ctrWeight': 'Float64',
    'discountWeight': 'Float64',
    'keywordSimilarityWeight': 'Float64',
    'sovWeightedWeight': 'Float64',
    'organicRankingWeight': 'Float64',
    'amazonBrandRankingWeight': 'Float64',
    'repeatedPurchaseWeight': 'Float64',
    'totalIroas': 'Float64',
    'spIroas': 'Float64',
    'sdIroas': 'Nullable(Float64)',
    'sbIroas': 'Float64',
    'dspIroas': 'Nullable(Float64)',
    'totalIroasFactor': 'Float64',
    'spIroasFactor': 'Float64',
    'sdIroasFactor': 'Nullable(Float64)',
    'sbIroasFactor': 'Float64',
    'dspIroasFactor': 'Nullable(Float64)',
    'totalAttributedSales': 'Float64',
    'spAttributedSales': 'Float64',
    'sdAttributedSales': 'Nullable(Float64)',
    'sbAttributedSales': 'Float64',
    'dspAttributedSales': 'Nullable(Float64)',
    'totalAzattributedSales': 'Float64',
    'spAzattributedSales': 'Float64',
    'sdAzattributedSales': 'Nullable(Float64)',
    'sbAzattributedSales': 'Float64',
    'dspAzattributedSales': 'Nullable(Float64)',
    'spSpend': 'Float64',
    'sdSpend': 'Nullable(Float64)',
    'sbSpend': 'Float64',
    'dspSpend': 'Nullable(Float64)',
    'cac': 'Float64',
    'totalSpend': 'Float64',
    'totalSales': 'Float64',
    'baselineShare': 'Float64',
    'spAttributedShare': 'Float64',
    'sbAttributedShare': 'Float64',
    'sdAttributedShare': 'Nullable(Float64)',
    'dspAttributedShare': 'Nullable(Float64)',
    'spCoef': 'Float64',
    'sbCoef': 'Float64',
    'sdCoef': 'Nullable(Float64)',
    'dspCoef': 'Nullable(Float64)',
    'otherCoef': 'Float64',
    'spLagweight': 'Float64',
    'sdLagweight': 'Nullable(Float64)',
    'sbLagweight': 'Float64',
    'dspLagweight': 'Nullable(Float64)',
    'spHalfmax': 'Float64',
    'sdHalfmax': 'Nullable(Float64)',
    'sbHalfmax': 'Float64',
    'dspHalfmax': 'Nullable(Float64)',
    'spSlope': 'Float64',
    'sdSlope': 'Nullable(Float64)',
    'sbSlope': 'Float64',
    'dspSlope': 'Nullable(Float64)',
    'status': 'UInt16',
    'version': 'UInt64',
    'sign': 'Int8',
    '_insert_time': 'DateTime'
}

def create_incrementality_table(client):
    try:
        client.execute('CREATE DATABASE IF NOT EXISTS incrementality')
        
        # 生成建表SQL
        columns = [f"{col} {dtype}" for col, dtype in TABLE_SCHEMA.items()]
        create_sql = f'''
            CREATE TABLE IF NOT EXISTS incrementality.incrementalityResult_all (
                {', '.join(columns)}
            ) ENGINE = MergeTree()
            ORDER BY (brandOriginalId, reportDate)
        '''
        client.execute(create_sql)
        print("Created incrementalityResult_all table successfully!")
    except Exception as e:
        print(f"Failed to create incrementalityResult_all table: {e}")

def test_sqlite_connection():
    try:
        conn = sqlite3.connect('src/backend/brands.db')
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print("Successfully connected to SQLite!")
        print("Tables in database:", tables)
    except Exception as e:
        print(f"Failed to connect to SQLite: {e}")
        return 
    finally:
        if conn:
            conn.close()

# 测试数据列表
TEST_DATA_LIST = [
    {
        'brandOriginalId': 1834604203458756610,
        'reportDate': datetime.strptime('2022-01-03', '%Y-%m-%d').date(),
        'date': datetime.strptime('2022-01-02', '%Y-%m-%d').date(),
        'year': 2022,
        'month': 1,
        'week': 202201,
        'quarter': 1,
    'updateTimestamp': datetime.strptime('2025-01-14 02:21:19', '%Y-%m-%d %H:%M:%S'),
    'updatedBy': 'test1',
    'spBrandedSearchPct': 0.5154,
    'sbBrandedPct': 0.0,
    'spNtbPct': 0.0,
    'dspNtbPct': 0.4073,
    'glanceViewPct': 0.9682,
    'ctr': 0.0042,
    'discount': None,
    'keywordSimilarity': None,
    'sovWeighted': None,
    'organicRanking': 2.1429,
    'amazonBrandRanking': None,
    'repeatedPurchase': 0.0,
    'spendWeight': 19.8091,
    'spBrandedSearchPctWeight': 0.0,
    'sbBrandedPctWeight': 0.0,
    'spNtbPctWeight': 0.0,
    'dspNtbPctWeight': -1.0,
    'glanceViewPctWeight': 0.7319,
    'ctrWeight': 0.4041,
    'discountWeight': 0.0672,
    'keywordSimilarityWeight': 0.0,
    'sovWeightedWeight': 0.0,
    'organicRankingWeight': 1.8077,
    'amazonBrandRankingWeight': 0.0,
    'repeatedPurchaseWeight': 0.8481,
    'totalIroas': 0.9859,
    'spIroas': 1.3473,
    'sdIroas': None,
    'sbIroas': 0.631,
    'dspIroas': None,
    'totalIroasFactor': 0.3918,
    'spIroasFactor': 0.4425,
    'sdIroasFactor': None,
    'sbIroasFactor': 0.316,
    'dspIroasFactor': None,
    'totalAttributedSales': 3094.4853,
    'spAttributedSales': 2095.339,
    'sdAttributedSales': None,
    'sbAttributedSales': 999.1463,
    'dspAttributedSales': None,
    'totalAzattributedSales': 7897.44,
    'spAzattributedSales': 4735.75,
    'sdAzattributedSales': None,
    'sbAzattributedSales': 3161.69,
    'dspAzattributedSales': None,
    'spSpend': 1555.22,
    'sdSpend': None,
    'sbSpend': 1583.45,
    'dspSpend': None,
    'cac': 3138.67,
    'totalSpend': 209114.82,
    'totalSales': 0.9852,
    'baselineShare': 0.01,
    'spAttributedShare': 0.0048,
    'sbAttributedShare': 0.0,
    'sdAttributedShare': None,
    'dspAttributedShare': 0.0159,
    'spCoef': 0.0086,
    'sbCoef': 0.0086,
    'sdCoef': 0.2273,
    'dspCoef': None,
    'otherCoef': 0.2673,
    'spLagweight': 0.2561,
    'sdLagweight': 0.1937,
    'sbLagweight': 0.9981,
    'dspLagweight': None,
    'spHalfmax': 0.9387,
    'sdHalfmax': 1.0191,
    'sbHalfmax': 0.8771,
    'dspHalfmax': None,
    'spSlope': 0.9651,
    'sdSlope': 0.1885,
    'sbSlope': 200,
    'dspSlope': 0.1,
    'status': 200,
    'version': 1736821288000,
    'sign': 1,
    '_insert_time': datetime.strptime('2025-01-14 02:21:28', '%Y-%m-%d %H:%M:%S')
},

    {
        'brandOriginalId': 1834604203458756611,
        'reportDate': datetime.strptime('2022-02-07', '%Y-%m-%d').date(),
        'date': datetime.strptime('2022-02-06', '%Y-%m-%d').date(),
        'year': 2022,
        'month': 2,
        'week': 202206,
        'quarter': 1,
        'updateTimestamp': datetime.strptime('2025-02-15 03:31:29', '%Y-%m-%d %H:%M:%S'),
        'updatedBy': 'test2',
        'spBrandedSearchPct': 0.6154,
        'sbBrandedPct': 0.1,
        'spNtbPct': 0.1,
        'dspNtbPct': 0.5073,
        'glanceViewPct': 0.8682,
        'ctr': 0.0052,
        'discount': None,
        'keywordSimilarity': None,
        'sovWeighted': None,
        'organicRanking': 2.2429,
        'amazonBrandRanking': None,
        'repeatedPurchase': 0.1,
        'spendWeight': 20.8091,
        'spBrandedSearchPctWeight': 0.1,
        'sbBrandedPctWeight': 0.1,
        'spNtbPctWeight': 0.1,
        'dspNtbPctWeight': -1.1,
        'glanceViewPctWeight': 0.8319,
        'ctrWeight': 0.5041,
        'discountWeight': 0.1672,
        'keywordSimilarityWeight': 0.1,
        'sovWeightedWeight': 0.1,
        'organicRankingWeight': 1.9077,
        'amazonBrandRankingWeight': 0.1,
        'repeatedPurchaseWeight': 0.9481,
        'totalIroas': 1.0859,
        'spIroas': 1.4473,
        'sdIroas': None,
        'sbIroas': 0.731,
        'dspIroas': None,
        'totalIroasFactor': 0.4918,
        'spIroasFactor': 0.5425,
        'sdIroasFactor': None,
        'sbIroasFactor': 0.416,
        'dspIroasFactor': None,
        'totalAttributedSales': 3194.4853,
        'spAttributedSales': 2195.339,
        'sdAttributedSales': None,
        'sbAttributedSales': 1099.1463,
        'dspAttributedSales': None,
        'totalAzattributedSales': 7997.44,
        'spAzattributedSales': 4835.75,
        'sdAzattributedSales': None,
        'sbAzattributedSales': 3261.69,
        'dspAzattributedSales': None,
        'spSpend': 1655.22,
        'sdSpend': None,
        'sbSpend': 1683.45,
        'dspSpend': None,
        'cac': 3238.67,
        'totalSpend': 210114.82,
        'totalSales': 1.0852,
        'baselineShare': 0.02,
        'spAttributedShare': 0.0148,
        'sbAttributedShare': 0.01,
        'sdAttributedShare': None,
        'dspAttributedShare': 0.0259,
        'spCoef': 0.0186,
        'sbCoef': 0.0186,
        'sdCoef': 0.3273,
        'dspCoef': None,
        'otherCoef': 0.3673,
        'spLagweight': 0.3561,
        'sdLagweight': 0.2937,
        'sbLagweight': 0.8981,
        'dspLagweight': None,
        'spHalfmax': 0.8387,
        'sdHalfmax': 1.1191,
        'sbHalfmax': 0.7771,
        'dspHalfmax': None,
        'spSlope': 0.8651,
        'sdSlope': 0.2885,
        'sbSlope': 210,
        'dspSlope': 0.2,
        'status': 200,
        'version': 1736821288001,
        'sign': 1,
        '_insert_time': datetime.strptime('2025-02-15 03:31:38', '%Y-%m-%d %H:%M:%S')
    },
    {
        'brandOriginalId': 1834604203458756610,
        'reportDate': datetime.strptime('2022-03-14', '%Y-%m-%d').date(),
        'date': datetime.strptime('2022-03-13', '%Y-%m-%d').date(),
        'year': 2022,
        'month': 3,
        'week': 202211,
        'quarter': 1,
        'updateTimestamp': datetime.strptime('2025-03-16 04:41:39', '%Y-%m-%d %H:%M:%S'),
        'updatedBy': 'test3',
        'spBrandedSearchPct': 0.7154,
        'sbBrandedPct': 0.2,
        'spNtbPct': 0.2,
        'dspNtbPct': 0.6073,
        'glanceViewPct': 0.7682,
        'ctr': 0.0062,
        'discount': None,
        'keywordSimilarity': None,
        'sovWeighted': None,
        'organicRanking': 2.3429,
        'amazonBrandRanking': None,
        'repeatedPurchase': 0.2,
        'spendWeight': 21.8091,
        'spBrandedSearchPctWeight': 0.2,
        'sbBrandedPctWeight': 0.2,
        'spNtbPctWeight': 0.2,
        'dspNtbPctWeight': -1.2,
        'glanceViewPctWeight': 0.9319,
        'ctrWeight': 0.6041,
        'discountWeight': 0.2672,
        'keywordSimilarityWeight': 0.2,
        'sovWeightedWeight': 0.2,
        'organicRankingWeight': 2.0077,
        'amazonBrandRankingWeight': 0.2,
        'repeatedPurchaseWeight': 1.0481,
        'totalIroas': 1.1859,
        'spIroas': 1.5473,
        'sdIroas': None,
        'sbIroas': 0.831,
        'dspIroas': None,
        'totalIroasFactor': 0.5918,
        'spIroasFactor': 0.6425,
        'sdIroasFactor': None,
        'sbIroasFactor': 0.516,
        'dspIroasFactor': None,
        'totalAttributedSales': 3294.4853,
        'spAttributedSales': 2295.339,
        'sdAttributedSales': None,
        'sbAttributedSales': 1199.1463,
        'dspAttributedSales': None,
        'totalAzattributedSales': 8097.44,
        'spAzattributedSales': 4935.75,
        'sdAzattributedSales': None,
        'sbAzattributedSales': 3361.69,
        'dspAzattributedSales': None,
        'spSpend': 1755.22,
        'sdSpend': None,
        'sbSpend': 1783.45,
        'dspSpend': None,
        'cac': 3338.67,
        'totalSpend': 211114.82,
        'totalSales': 1.1852,
        'baselineShare': 0.03,
        'spAttributedShare': 0.0248,
        'sbAttributedShare': 0.02,
        'sdAttributedShare': None,
        'dspAttributedShare': 0.0359,
        'spCoef': 0.0286,
        'sbCoef': 0.0286,
        'sdCoef': 0.4273,
        'dspCoef': None,
        'otherCoef': 0.4673,
        'spLagweight': 0.4561,
        'sdLagweight': 0.3937,
        'sbLagweight': 0.7981,
        'dspLagweight': None,
        'spHalfmax': 0.7387,
        'sdHalfmax': 1.2191,
        'sbHalfmax': 0.6771,
        'dspHalfmax': None,
        'spSlope': 0.7651,
        'sdSlope': 0.3885,
        'sbSlope': 220,
        'dspSlope': 0.3,
        'status': 200,
        'version': 1736821288002,
        'sign': 1,
        '_insert_time': datetime.strptime('2025-03-16 04:41:48', '%Y-%m-%d %H:%M:%S')
    }
]

def insert_incrementality_data(client):
    try:
        # 将数据字典转换为元组列表
        data = [tuple(test_data.values()) for test_data in TEST_DATA_LIST]
        client.execute('INSERT INTO incrementality.incrementalityResult_all VALUES', data, types_check=True)
        print(f"Inserted {len(TEST_DATA_LIST)} test data records into incrementalityResult_all successfully!")
    except Exception as e:
        print(f"Failed to insert test data: {e}")

if __name__ == "__main__":
    # print("Testing SQLite connection...")
    # test_sqlite_connection()
    print("\nTesting ClickHouse connection...")
    client = Client(
        host='localhost',
        port=9000,
        user='default',
        password='123456'
    )
    test_clickhouse_connection()
    print("\nCreating incrementality table...")
    create_incrementality_table(client)
    print("\nInserting test data...")
    insert_incrementality_data(client)
    
    # 查询插入的数据
    print("\nQuerying test data...")
    result = client.execute('SELECT * FROM incrementality.incrementalityResult_all WHERE brandOriginalId = %(brand_id)s',
                           {'brand_id': TEST_DATA_LIST[0]['brandOriginalId']})
    print("Query result:")
    for row in result:
        print(row)
