import os

LKL_HOST = os.getenv('LKL_HOST', 'http://86.57.193.146:50052')
CH_HOST = os.getenv('CH_HOST', 'http://0.0.0.0:8123')
START_BLOCK = os.getenv('START_BLOCK', None)
BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', '0.0.0.0:9092')
SLEEP_TIME = int(os.getenv('SLEEP_TIME', 5))
KAFKA_TOPIC_BLOCKS = os.getenv('KAFKA_TOPIC_BLOCKS', 'blocks')
KAFKA_TOPIC_TRANSACTIONS = os.getenv('KAFKA_TOPIC_TRANSACTIONS', 'transactions')
CH_SYNK_TABLE = os.getenv('CH_SYNK_TABLE', 'transactions')
SAFETY_MARGIN = int(os.getenv('SAFETY_MARGIN', '3'))
