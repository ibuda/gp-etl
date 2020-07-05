import pymongo
import constants as c
from bulk_queries import BulkQueries
import copy
from logger import logger

pipe_ins = c.PIPE_INS
pipe_upd = c.PIPE_UPD
pipe_usr = c.PIPE_USR
dt_format = c.DT_FORMAT
LOGGER = logger('etl')


def run_all(dt_from, dt_to):
    myclient = pymongo.MongoClient(c.CREDS['mongo'])
    db = myclient[c.CREDS['mongo_db']]
    bulkq = BulkQueries(table='user_orders', columns=c.COLS)
    LOGGER.info(
        f"iteration for date from: {dt_from.strftime(dt_format)} started")
    update_pipes(dt_from, dt_to)

    doc_ins = db.orders.aggregate(pipe_ins)
    for doc in doc_ins:
        bulkq.insert_row(doc)
    count = len(bulkq.insert_rows)
    LOGGER.info(f"inserting {len(bulkq.insert_rows)} rows")
    bulkq.run()
    bulkq.insert_rows.clear()

    # upserting the newly updated orders records
    doc_upd = db.orders.aggregate(pipe_upd)
    for doc in doc_upd:
        doc['update_columns'] = c.COLS_UPDATE
        bulkq.upsert_row(doc)
    count += len(bulkq.upsert_rows)
    LOGGER.info(f"upserting {len(bulkq.upsert_rows)} rows")
    bulkq.run()
    bulkq.upsert_rows.clear()

    # updating all rows with updated user info
    doc_usr = db.users.aggregate(pipe_usr)
    for doc in doc_usr:
        doc.pop('_id')
        doc.pop('created_at')
        doc['id_col'] = 'user_id'
        doc['user_updated_at'] = doc.pop('updated_at')
        bulkq.update_row(doc)
    count += len(bulkq.update_rows)
    LOGGER.info(f"updating {len(bulkq.update_rows)} rows")
    bulkq.run()
    bulkq.update_rows.clear()
    LOGGER.info(
        f"iteration for date_from: {dt_from.strftime(dt_format)} completed")

    myclient.close()

    # returning whether any changes where introduced
    return count > 0


def update_pipes(dt_from, dt_to):
    dtfrom = dt_from.strftime(dt_format)
    dtto = dt_to.strftime(dt_format)
    dt_range = {"$gte": dtfrom, "$lt": dtto}
    pipe_ins[0]['$match']['created_at'] = dt_range

    pipe_upd[0]['$match']['created_at']['$lt'] = dtfrom
    dtcols = ['date_tz', 'updated_at', 'fullfillment_date_tz']

    pipe_upd[0]['$match']['$or'] = [{col: dt_range} for col in dtcols]

    pipe_usr[0]['$match']['created_at']['$lt'] = dtfrom
    pipe_usr[0]['$match']['updated_at'] = dt_range
