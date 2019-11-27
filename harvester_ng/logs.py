import logging
import logging.config

d = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'detailed': {
            'class': 'logging.Formatter',
            'format': '%(asctime)s %(name)-15s %(funcName)-15s %(levelname)-8s %(message)s'
        },
        'console': {
            'class': 'logging.Formatter',
            'format': '%(asctime)s %(name)-15s %(funcName)-15s %(levelname)-8s %(message)s',
            'datefmt': '%H:%M:%S'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'console',
        },
        'file': {
            'class': 'logging.FileHandler',
            'filename': 'harvester_ng.log',
            'mode': 'w',
            'level': 'DEBUG',
            'formatter': 'detailed',
        },
    },
    'loggers': {  
        'harvester_ng': {
            'level': 'INFO',
            'handlers': ['file', 'console'],
            'propagate': False,
        },
        'harvesters': {
            'level': 'INFO',
            'handlers': ['file', 'console'],
            'propagate': False
        },
        'harvester_adapters': {
            'level': 'INFO',
            'handlers': ['file', 'console'],
            'propagate': False
        },
        'dataflows': {
            'level': 'INFO',
            'handlers': ['file', 'console'],
            'propagate': False
        },
        '': {
            'level': 'INFO',
            'handlers': ['file', 'console'],
        },
    }
}

logging.config.dictConfig(d)
logger = logging.getLogger(__name__)