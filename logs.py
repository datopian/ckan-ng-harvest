import logging
logger = logging.getLogger(__name__)

c_handler = logging.StreamHandler()
f_handler = logging.FileHandler('harvest.log')
logger.addHandler(c_handler)
logger.addHandler(f_handler)
logger.setLevel(logging.DEBUG)