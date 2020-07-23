import os

LKL_HOST = os.getenv('LKL_HOST', 'http://86.57.193.146:50052')
CH_HOST = os.getenv('CH_HOST', '0.0.0.0')
START_BLOCK = int(os.getenv('START_BLOCK', '0'))
WAIT_BLOCK_POLICY = os.getenv('WAIT_BLOCK_POLICY', 'wait')
