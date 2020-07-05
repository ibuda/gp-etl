import json
import copy
from datetime import datetime

with open('assets/creds.json') as f:
    CREDS = json.load(f)

DT_FORMAT = '%Y-%m-%d %H:%M:%S'
# starting datetime points for scheduler
DT_FROM = datetime(2019, 6, 18, 0, 0, 0)  # "2019-06-18 00:00:00"
DT_TO = datetime(2019, 6, 20, 0, 0, 0)  # "2019-06-20 00:00:00"

# aggregation pipe used to fetch new orders
PIPE_INS = [
    {'$match': {"created_at": {"$gte": '2019:01:01', "$lt": '2020-01-01'}}},
    {'$lookup': {
        'from': 'users',
        'localField': 'user_id',
        'foreignField': 'user_id',
        'as': 'user'
    }
    },
    {
        '$unwind': '$user',
        '$unwind': {
            'path': "$user",
            'preserveNullAndEmptyArrays': True
        }
    },
    {'$project': {
        '_id': 0,
        'id': 1,
        'created_at': 1,
        'date_tz': 1,
        'item_count': 1,
        'order_id': 1,
        'receive_method': 1,
        'status': 1,
        'store_id': 1,
        'subtotal': 1,
        'tax_percentage': 1,
        'total': 1,
        'total_discount': 1,
        'total_gratuity': 1,
        'total_tax': 1,
        'updated_at': 1,
        'user_id': 1,
        'fulfillment_date_tz': 1,
        'status': 1,
        'first_name': {'$ifNull': ['$user.first_name', None]},
        'last_name': {'$ifNull': ['$user.last_name', None]},
        'merchant_id': {'$ifNull': ['$user.merchant_id', None]},
        'phone_number': {'$ifNull': ['$user.phone_number', None]},
        'user_created_at': {'$ifNull': ['$user.created_at', None]},
        'user_updated_at': {'$ifNull': ['$user.updated_at', None]},
    }
    }
]

# aggregation pipe used to fetch orders with updated info
PIPE_UPD = [
    {'$match': {
        "created_at": {"$lt": '2020-01-01'},
        "$or": [
            {"date_tz": {"$gte": '2019-01-01', "$lt": '2020-01-01'}},
            {"updated_at": {"$gte": '2019-01-01', "$lt": '2020-01-01'}},
            {"fullfillment_date_tz": {"$gte": '2019-01-01', "$lt": '2020-01-01'}},
        ]
    }
    },
    {'$lookup': {
        'from': 'users',
        'localField': 'user_id',
        'foreignField': 'user_id',
        'as': 'user'
    }
    },
    {
        '$unwind': '$user',
        '$unwind': {
            'path': "$user",
            'preserveNullAndEmptyArrays': True
        }
    },
    {'$project': {
        '_id': 0,
        'id': 1,
        'created_at': 1,
        'date_tz': 1,
        'item_count': 1,
        'order_id': 1,
        'receive_method': 1,
        'status': 1,
        'store_id': 1,
        'subtotal': 1,
        'tax_percentage': 1,
        'total': 1,
        'total_discount': 1,
        'total_gratuity': 1,
        'total_tax': 1,
        'updated_at': 1,
        'user_id': 1,
        'fulfillment_date_tz': 1,
        'status': 1,
        'first_name': {'$ifNull': ['$user.first_name', None]},
        'last_name': {'$ifNull': ['$user.last_name', None]},
        'merchant_id': {'$ifNull': ['$user.merchant_id', None]},
        'phone_number': {'$ifNull': ['$user.phone_number', None]},
        'user_created_at': {'$ifNull': ['$user.created_at', None]},
        'user_updated_at': {'$ifNull': ['$user.updated_at', None]},
    }
    }
]

# aggregation pipe used to fetch users with updated info
PIPE_USR = [
    {'$match': {
        "created_at": {"$lt": '2019-01-01'},
        "updated_at": {"$gte": '2019-01-01', "$lt": '2020-01-01'}
    }
    }
]


# user_orders table columns
COLS = [
    'id',
    'created_at',
    'date_tz',
    'item_count',
    'order_id',
    'receive_method',
    'status',
    'store_id',
    'subtotal',
    'tax_percentage',
    'total',
    'total_discount',
    'total_gratuity',
    'total_tax',
    'updated_at',
    'user_id',
    'fulfillment_date_tz',
    'first_name',
    'last_name',
    'merchant_id',
    'phone_number',
    'user_created_at',
    'user_updated_at'
]

# columns to update in postgres user_orders table
# atm taking all but id columns
COLS_UPDATE = copy.deepcopy(COLS).remove('id')
