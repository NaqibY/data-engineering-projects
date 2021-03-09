import logging

def logger(filename,filemode):
    '''
        Basic logging file
            initailise the logger by adding logger=logger() at the top of your script

        Args:
            filename = default 'logs.log'
            filemode = default 'a'
        '''
    filename='logs.log'
    filemode='a'
    logging.basicConfig(filename=filename,level=logging.INFO,format='%(asctime)s %(levelname)-8s %(message)s',datefmt='%a, %d %b %Y %H:%M:%S', filemode=filemode)
    
    logger=logging.getLogger(__name__)

    return logger