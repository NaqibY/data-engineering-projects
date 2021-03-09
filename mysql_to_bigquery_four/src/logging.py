import logging

def logger(filename):

    logging.basicConfig(filename=filename, level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%a, %d %b %Y %H:%M:%s')
    logger=logging.getLogger(__name__)

    return logger
