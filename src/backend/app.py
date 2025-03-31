from flask import Flask, jsonify, request
from flask_cors import CORS
from clickhouse_driver import Client
import logging
from datetime import datetime

# 从test_db_connection.py中导入TEST_DATA
TEST_DATA = {
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
}

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
app.config['JSON_SORT_KEYS'] = False
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
app.config['JSON_AS_ASCII'] = False
app.config['ENV'] = 'production' if not app.debug else 'development'

CORS(app, resources={r"/api/v1/*": {"origins": "*"}})

def get_db_connection():
    try:
        client = Client(
            host='localhost',
            port=9000,
            user='default',
            password='123456',
            database='incrementality'
        )
        return client
    except Exception as e:
        app.logger.error(f"ClickHouse connection error: {str(e)}")
        raise

@app.route('/')
def health_check():
    return jsonify({
        'status': 'healthy',
        'service': 'incrementality backend',
        'version': '1.0.0'
    })

@app.route('/api/v1/brands', methods=['GET'])
def get_brands():
    try:
        search = request.args.get('search', '')
        client = get_db_connection()
        
        if search:
            query = '''
                SELECT 
                    brand_id, 
                    brand_name, 
                    totalIroas, 
                    spIroas, 
                    sdIroas, 
                    sbIroas, 
                    dspIroas
                FROM brands 
                WHERE brand_id = %(brand_id)s 
                OR brand_name ILIKE %(search)s
            '''
            params = {'brand_id': search, 'search': f'%{search}%'}
        else:
            query = '''
                SELECT 
                    brand_id, 
                    brand_name, 
                    totalIroas, 
                    spIroas, 
                    sdIroas, 
                    sbIroas, 
                    dspIroas
                FROM brands
            '''
            params = {}
            
        brands = client.execute(query, params)
        return jsonify({
            'data': [dict(zip(
                ['brand_id', 'brand_name', 'totalIroas', 'spIroas', 'sdIroas', 'sbIroas', 'dspIroas'], 
                brand
            )) for brand in brands],
            'message': 'Success'
        })
    except Exception as e:
        app.logger.error(f"Error in get_brands: {str(e)}")
        return jsonify({
            'data': [],
            'message': 'Error occurred while fetching brands'
        }), 500

@app.route('/api/v1/metrics/<int:brand_id>', methods=['GET'])
def get_metrics(brand_id):
    try:
        client = get_db_connection()
        query = '''
            SELECT 
                totalIroas,
                spIroas,
                sdIroas,
                sbIroas,
                dspIroas
            FROM brands
            WHERE brandOriginalId = %(brand_id)s
        '''
        metrics = client.execute(query, {'brand_id': brand_id})
        
        if metrics:
            return jsonify({
                'data': dict(zip(
                    ['totalIroas', 'spIroas', 'sdIroas', 'sbIroas', 'dspIroas'],
                    metrics[0]
                )),
                'message': 'Success'
            })
        else:
            return jsonify({
                'data': None,
                'message': 'Brand not found'
            }), 404
    except Exception as e:
        app.logger.error(f"Error in get_metrics: {str(e)}")
        return jsonify({
            'data': None,
            'message': 'Error occurred while fetching metrics'
        }), 500

@app.route('/api/v1/metrics/<int:brand_id>/weekly', methods=['GET'])
def get_weekly_metrics(brand_id):
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        client = get_db_connection()
        
        query = '''
            SELECT 
                reportDate,
                totalIroas,
                spIroas,
                sdIroas,
                sbIroas,
                dspIroas
            FROM incrementality.incrementalityResult_all
            WHERE brandOriginalId = %(brand_id)s
        '''
        params = {'brand_id': brand_id}
        
        if start_date and end_date:
            query += ' AND reportDate BETWEEN %(start_date)s AND %(end_date)s'
            params.update({'start_date': start_date, 'end_date': end_date})
        elif start_date:
            query += ' AND reportDate >= %(start_date)s'
            params['start_date'] = start_date
        elif end_date:
            query += ' AND reportDate <= %(end_date)s'
            params['end_date'] = end_date
            
        query += ' ORDER BY reportDate ASC'
        
        metrics = client.execute(query, params)
        
        data = [{
            'reportDate': metric[0],
            'totalIroas': float(metric[1]) if metric[1] is not None else None,
            'spIroas': float(metric[2]) if metric[2] is not None else None,
            'sdIroas': float(metric[3]) if metric[3] is not None else None,
            'sbIroas': float(metric[4]) if metric[4] is not None else None,
            'dspIroas': float(metric[5]) if metric[5] is not None else None
        } for metric in metrics]
        
        return jsonify({
            'data': data,
            'message': 'Success'
        })
    except Exception as e:
        app.logger.error(f"Error in get_weekly_metrics: {str(e)}")
        return jsonify({
            'data': [],
            'message': 'Error occurred while fetching weekly metrics'
        }), 500

@app.route('/api/v1/test-data', methods=['POST'])
def insert_test_data():
    try:
        client = get_db_connection()
        # 插入测试数据
        data = [tuple(TEST_DATA.values())]
        client.execute('INSERT INTO incrementality.incrementalityResult_all VALUES', data, types_check=True)
        return jsonify({
            'message': 'Test data inserted successfully'
        })
    except Exception as e:
        app.logger.error(f"Error inserting test data: {str(e)}")
        return jsonify({
            'message': f'Error occurred while inserting test data: {str(e)}'
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
