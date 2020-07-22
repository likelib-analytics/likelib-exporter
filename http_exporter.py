import json
import logging
import requests as r
from time import sleep
from clickhouse_driver import Client

from config import LKL_HOST, CH_HOST, START_BLOCK, WAIT_BLOCK_POLICY


client = Client(host=CH_HOST)
logging.basicConfig(level=logging.INFO)
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


def get_block(
    b_number: int,
    sleep_time: int = 1,
    wait_block_policy: str = 'wait'
):
    ''' Gets block by number. If the the block is not mined yet and `wait_block_policy` == 'wait',
        wait for new block `sleep_time` seconds and repeats itself.
        If `wait_block_policy` != 'wait', stops the exporter.
    '''
    block_data = send_query('/get_block', {'number': b_number})

    if block_data['status'] == 'error':
        assert block_data['result'] == f'Block was not found. number: {b_number}', \
            ('Unknown result:', block_data['result'])
        # if block is not mined yet:
        if wait_block_policy == 'wait':
            logging.info('Waiting for the block...')
            sleep(sleep_time)
            get_block(b_number, sleep_time)
        else:
            return False

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
    return [block_dict]


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


def push_data(data, table):
    ''' Pushes data to ClickHouse.
        TODO: push data to kafka instead of CH.
    '''
    client.execute(
        f'''INSERT INTO {table} ({', '.join(tuple(data[0].keys()))}) VALUES''',
        data
    )


def parse_save_block(block_raw):
    ''' Parses block into desired data schemas. '''
    block_data = parse_block_data(block_raw)
    push_data(block_data, table='blocks')

    trx_data = parse_transaction_data(block_raw)
    push_data(trx_data, table='transactions')


def exporter(
    start_block: int,
    exit_block: [int, None] = None,
    wait_block_policy: str = 'wait'
):
    ''' Exports blocks from LikeLib node into local database. '''
    if exit_block:
        assert start_block < exit_block, 'Wrong exit_block is supplied!'

    while True:
        block = get_block(start_block, wait_block_policy=wait_block_policy)
        if start_block % 100 == 0:
            logging.info(f'Processed blocks till block {start_block}. Continuing...')
        if block:
            parse_save_block(block)
        else:
            logging.info('Exited exporter. Last processed block:', start_block)
            break

        start_block += 1
        if start_block == exit_block:
            logging.info('Exiting on block:', exit_block)
            break


def main():
    logging.info(f'Running exporter from block: {START_BLOCK}')
    exporter(START_BLOCK, wait_block_policy=WAIT_BLOCK_POLICY)


if __name__ == '__main__':
    main()
