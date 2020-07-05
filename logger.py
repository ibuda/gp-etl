import os
import logging
import logging.config

PROJ_FOLDER = os.path.dirname(os.path.abspath(__file__)) + '/'


def logger(name):
    logging.config.fileConfig(f'{PROJ_FOLDER}logger.conf')
    logging.getLogger('apscheduler.executors.default').setLevel(logging.ERROR)
    LOGGER = logging.getLogger(name)

    return LOGGER
