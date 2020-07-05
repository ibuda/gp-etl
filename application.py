from flask import Flask
from flask_apscheduler import APScheduler
from applicationinsights.flask.ext import AppInsights
import etl
from datetime import timedelta
import constants as c
from logger import logger

LOGGER = logger('app')

app = Flask(__name__)
app.config['APPINSIGHTS_INSTRUMENTATIONKEY'] = 'b0bbcab2-4ac6-4206-9dab-2747135d894b'
appinsights = AppInsights(app)
scheduler = APScheduler()
dt_from = c.DT_FROM
dt_to = c.DT_TO
counter = 0


@app.route("/")
def index():
    message = "Welcome to the ETL project!<br/>"
    message += f"Approximately {counter} ETLs performed atm<br/>"
    message += f"Next ETL Scheduled for : {dt_from.strftime(c.DT_FORMAT)}"

    return message


@app.route("/schedule_etl_jobs")
def start_etl():
    job = scheduler.get_job('Scheduled ETL')
    if job is None:
        scheduler.add_job(id='Scheduled ETL', func=scheduledTask,
                          trigger='interval', minutes=1)
        scheduler.start()
        message = f'Initiated job scheduler with interval 1 minute'
    else:
        message = 'Job scheduler initiated successfully'

    return message


def scheduledTask():
    global dt_from, dt_to, counter
    # running etl job
    LOGGER.info(f'Running scheduled stask for {dt_from.strftime(c.DT_FORMAT)}')
    status = etl.run_all(dt_from, dt_to)
    # setting date range for the next etl if previous had an effect
    counter += status
    if status:
        dt_from = dt_to
        dt_to += timedelta(days=10)


if __name__ == '__main__':
    # scheduler.add_job(id='Scheduled ETL', func=scheduledTask,
    #                   trigger='interval', minutes=1)
    # scheduler.start()
    # app.run(host='0.0.0.0', port=8080)
    pass
