import json
import hashlib
import logging
import requests as r
from time import sleep
from kafka import KafkaProducer

from config import \
    LKL_HOST, \
    BOOTSTRAP_SERVERS, \
    START_BLOCK, \
    SLEEP_TIME, \
    KAFKA_TOPIC_BLOCKS, \
    KAFKA_TOPIC_TRANSACTIONS, \
    CH_SYNK_TABLE, \
    CH_HOST


# common logs
logging.basicConfig(level=logging.INFO)
# kafka logs
logger = logging.getLogger('kafka')
logger.setLevel(logging.WARN)

known_hashes = ()


def send_query(
    method: str,
    data: (str or dict),
    host: str = LKL_HOST,
):
    ''' Sends post query to the node via http.'''
    resp = r.post(
        url=host + method,
        headers={'Content-Type': 'application/json'},
        data=json.dumps(data)
    )

    assert resp.status_code == 200, ('Invalid status code:', resp.status_code)
    return resp.json()


def get_block(b_number: int,):
    ''' Gets block by number. If the the block is not mined yet, returns None.'''
    block_data = send_query('/get_block', {'number': b_number})

    if block_data['status'] == 'error':
        assert block_data['result'] == f'Block was not found. number:{b_number}', \
            ('Unknown result:', block_data['result'])

        # block is not mined yet.
        return None

    return block_data['result']


def parse_block_data(block: dict):
    ''' Prepares block data to be pushed into database.'''
    block_dict = {
        'depth': int(block['depth']),
        'coinbase': block['coinbase'],
        'nonce': int(block['nonce']),
        'previous_block_hash': block['previous_block_hash'],
        'dt': int(block['timestamp']),
    }
    return block_dict


def parse_transaction_data(block_raw: dict):
    ''' Parses transactions from a given blocks.
        There's an issue around LikeLib node's interface: the `get_block` method does not return transaction
        hashes. The only way to obtain trx hash with current interface is to get all transactions made by the address
        (either from or to) than iterate them and find a transaction with needed timestamp.
    '''
    trx_data = []

    for trx in block_raw['transactions']:
        if block_raw['depth'] == 0:
            trx_hash = 'genesis'  # trxHash in genesis block is None
        else:
            trx_hashes = get_address_transactions(trx['from'])
            trx_hash = find_trx_with_ts(trx_hashes, trx['timestamp'])

        trx_data.append({
            'transactionHash': trx_hash,
            'type': 'call',
            'from': trx['from'],
            'to': trx['to'],
            'amount': int(trx['amount']),
            'data': trx['data'],
            'fee': int(trx['fee']),
            'dt': trx['timestamp'],
            'depth': block_raw['depth']
        })

    return trx_data


def find_trx_with_ts(hashes: list, ts: int):
    ''' Iterates over all addresses' transactions and finds transaction with timestamp corresponding to
        current block's timestamp. In order to minimize amount of queries to LikeLib node saves transaction
        hashes from previous transactions.
        TODO: Once the node's interface is changed, remove this part.
    '''
    global known_hashes
    for trx_hash in hashes:
        if trx_hash not in known_hashes:
            raw_trx = send_query(method='/get_transaction', data={'hash': trx_hash})
            if raw_trx['result']['timestamp'] == ts:
                known_hashes += (trx_hash, )
                return trx_hash


def get_address_transactions(address: str):
    ''' Gets all transactions made by address.
        NOTE: there's a typo in likelib docs, the method is called `get_account` not `get_account_info`.
    '''
    address_data = send_query('/get_account', {'address': address})

    if address_data['status'] == 'error':
        raise Exception('Error status:', address_data['result'])
    if address_data['status'] != 'ok':
        logging.warning(f"Unknown status: {address_data['status']}. Result: {address_data['result']}")

    return address_data['result']['transaction_hashes']


def push_data(data, producer, topic, key):
    ''' Pushes data into kafka.'''
    producer.send(
        topic,
        key=json.dumps(key).encode('utf-8'),
        value=json.dumps(data).encode('utf-8')
    )


def get_kafka_key(data: dict):
    ''' In some cases the transaction hash might correspond to a few logical transactions. (e.g.
        miners' commission or contract calls. Using md5 as a key helps to avoid any data loss.
    '''
    m = hashlib.md5()
    m.update(''.join(str(v) for v in data.values()).encode())
    return m.hexdigest()


def parse_save_block(block_raw, kafka_producer):
    ''' Parses block into desired data schemas. '''
    block_data = parse_block_data(block_raw)
    push_data(block_data, producer=kafka_producer, topic=KAFKA_TOPIC_BLOCKS, key=block_data['depth'])

    trx_data = parse_transaction_data(block_raw)
    for trx in trx_data:
        push_data(trx, producer=kafka_producer, topic=KAFKA_TOPIC_TRANSACTIONS, key=get_kafka_key(trx))
    kafka_producer.flush()  # make sure all messages are delivered.


def exporter(start_block: int):
    ''' Exports blocks from LikeLib node into local database.'''

    while True:
        kafka_producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
        block = get_block(start_block)
        if block:
            parse_save_block(block, kafka_producer)
        else:
            logging.info(f'Waiting for the block #{start_block}')
            sleep(SLEEP_TIME)

        if start_block % 100 == 0:
            logging.info(f'Processed blocks till block #{start_block}. Continuing...')

        start_block += 1


def recover_progress(host):
    ''' Recovers progress by getting last processed block from ClickHouse. Starts with 0 if fails.'''
    resp = r.post(url=host, data=f'SELECT max(depth) FROM {CH_SYNK_TABLE}'.encode())
    if resp.ok:
        return resp.json()

    logging.warn("Failed to get last block from CliskHouse. Starting with block 0.")
    return 0


def main(start_block):
    if not start_block:  # Recover pregress
        start_block = recover_progress(CH_HOST)
    logging.info(f'Running exporter from block: {start_block}')
    exporter(start_block)


if __name__ == '__main__':
    main(START_BLOCK)
