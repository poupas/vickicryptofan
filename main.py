#!/usr/bin/env python

import time
import json
import re
import logging
import sys

from decimal import Decimal as D

import krakenex
import dateutil.parser

import twitter

import local_settings

VICKI_USER = '@vickicryptobot'

WAIT_SECONDS = 120

PAIR_POSITIONS = None

STATE_PATH = 'state.dat'
KRAKEN_AUTH_PATH = 'kraken.auth'


PAIRS = {
    'BTCUSD': {
        'buy': D(200),
        'kraken_pair': 'XBTEUR',
        'kraken_asset': 'XXBT',
        'asset': 'XBT'
    },

    'ETHUSD': {
        'buy': D(500),
        'kraken_pair': 'ETHEUR',
        'kraken_asset': 'XETH',
        'asset': 'ETH'
    },
    'XMRBTC': {
        'buy': D(100),
        'kraken_pair': 'XMREUR',
        'kraken_asset': 'XXMR',
        'asset': 'XMR'
    },
    'ZECUSD': {
        'buy': D(100),
        'kraken_pair': 'ZECEUR',
        'kraken_asset': 'XZEC',
        'asset': 'ZEC'
    },
}

KRAKEN_PAIRS = {
    'XBTEUR': 'BTCUSD',
    'ETHEUR': 'ETHUSD',
    'XMREUR': 'XMRBTC',
    'ZECEUR': 'ZECUSD',
}

EQUIVALENT_PAIRS = {
    'ETHBTC': 'ETHUSD'
}

REX = re.compile(
    '^I am\s+going\s+(?P<pos>short|long)(?:\s+on)?\s+(?P<pair>[A-Z]+)')


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


class KrakenError(Exception):
    pass


def update_state(state, pair, who, _id, what):
    if state is None:
        state = {}
    if pair not in state:
        state[pair] = {}
    if who not in state[pair]:
        state[pair][who] = {}

    curid = state[pair][who].get('id')
    if curid is None or curid <= _id:
        state[pair][who] = what

    return state


def load_state(path):
    try:
        with open(path, 'r') as fp:
            return json.load(fp)
    except (ValueError, IOError, TypeError):
        return None


def save_state(path, state):
    with open(path, 'w', encoding='ascii') as fp:
        json.dump(state, fp)


def vicki_refresh_pos(api, state, cur_tweet_id):

    log.debug('Fetching tweets...')
    timeline = api.GetUserTimeline(
        screen_name=VICKI_USER, exclude_replies=True, include_rts=False,
        trim_user=True, since_id=cur_tweet_id)

    for tweet in timeline:
        _id = int(tweet.id_str)
        cur_tweet_id = max(cur_tweet_id, _id)

        action = REX.match(tweet.text)
        if action is None:
            continue

        pair = action.group('pair')
        pair = EQUIVALENT_PAIRS.get(pair, pair)
        position = action.group('pos').lower()
        if pair not in PAIRS:
            continue

        ts = dateutil.parser.parse(tweet.created_at).strftime('%s')
        vicki_action = {
            'position': position,
            'ts': ts,
            'id': _id
        }
        state = update_state(state, pair, 'vicki', _id, vicki_action)

    return state, cur_tweet_id


def kraken_fetch_open_orders(api):
    response = api.query_private('OpenOrders', {})
    if response['error']:
        log.error('Could not fetch open orders: %s', response['error'])
        raise KrakenError(response['error'])

    try:
        open_orders = response['result']['open']
    except KeyError:
        return {}

    orders = {}
    for txid, order in open_orders.items():
        pair = order['descr']['pair']
        pair = KRAKEN_PAIRS.get(pair, pair)
        otype = order['descr']['type']
        if pair not in orders:
            orders[pair] = []

        orders[pair].append({'txid': txid, 'type': otype})

    return orders


def kraken_add_order(api, pair, _type, amount, otype='market'):

    args = {
        'pair': pair,
        'type': _type,
        'ordertype': otype,
        'volume': amount,
    }

    if otype == 'limit':
        bid, ask = kraken_pair_value(api, pair)
        args['price'] = ask if _type == 'buy' else bid

    response = api.query_private('AddOrder', args)
    if response['error']:
        log.error('Could not add order %s', response['error'])
        raise KrakenError(response['error'])

    log.info("Order %s was created", response['result'])
    return response['result']


def kraken_cancel_order(api, txid):
    response = api.query_private('CancelOrder', {'txid': txid})
    if response['error']:
        log.error('Could not cancel order %s', response['error'])
        return False

    log.info("Order %s was cancelled.", txid)
    return True


def kraken_fetch_balance(api):
    response = api.query_private('Balance', {})
    if response['error']:
        log.error('Could not fetch balance: %s', response['error'])
        raise KrakenError(response['error'])

    return(response['result'])


def kraken_fetch_asset_balance(api, asset):
    try:
        balance = kraken_fetch_balance(api)
        log.info('Current balance: %s', balance)
        balance = balance[asset]
    except KeyError:
        balance = '0'
    return D(balance)


def kraken_orders_to_pos(orders):
    positions = {}
    for pair, pair_orders in orders.items():
        positions[pair] = {}
        txids = []
        t = None
        for order in pair_orders:
            txids.append(order['txid'])

            if t is None:
                t = order['type']
                continue

            assert(order['type'] == t)

        positions[pair]['position'] = 'long' if t == 'buy' else 'short'
        positions[pair]['txids'] = txids

    return positions


def kraken_refresh_pos(state, api):
    orders = kraken_fetch_open_orders(api)
    positions = kraken_orders_to_pos(orders)
    for pair, position in positions.items():
        state = update_state(state, pair, 'kraken', None, position)

    return state


def kraken_pair_value(api, pair):
    response = api.query_public('Ticker', {'pair': pair})
    data = response['result'].popitem()[1]
    bid = D(data['b'][0])
    ask = D(data['a'][0])

    return bid, ask


def trading_state_machine(state, kapi):
    for pair, cfg in PAIRS.items():
        pair_state = state.get(pair)
        if pair_state is None:
            log.info('Ignoring pair %s...', pair)
            continue

        vicki = pair_state.get('vicki')
        if vicki is None:
            log.warn('Vicki does not recognize %s as a valid pair', pair)
            continue

        assert(vicki['position'] in ('short', 'long'))

        kraken = pair_state.get('kraken')
        if kraken is not None:
            if kraken['position'] == vicki['position']:
                log.info('Kraken and Vicki synced for pair %s', pair)
                continue

            # Vicki and Kraken disagree. Clear open orders for this pair.
            txids = kraken.get('txids', ())
            for txid in txids:
                log.info('Removing txid %s from open orders', txid)
                kraken_cancel_order(kapi, txid)
        else:
            kraken = pair_state['kraken'] = {}

        kraken_pair = cfg['kraken_pair']
        kraken_asset = cfg['kraken_asset']

        if vicki['position'] == 'long':
            asset_amount = kraken_fetch_asset_balance(kapi, kraken_asset)
            _, ask = kraken_pair_value(kapi, kraken_pair)
            asset_amount_base = ask * asset_amount
            max_spend = cfg['buy'] - asset_amount_base
            to_buy = max_spend / ask
            if to_buy > D('0.0001'):
                kraken_add_order(kapi, kraken_pair, 'buy', to_buy)
            kraken['txids'] = []
            kraken['position'] = 'long'

        elif vicki['position'] == 'short':
            to_sell = kraken_fetch_asset_balance(kapi, kraken_asset)
            if to_sell > D('0.0001'):
                kraken_add_order(kapi, kraken_pair, 'sell', to_sell)
            kraken['position'] = 'short'
            kraken['txids'] = []

    return state


def main():

    tapi = twitter.Api(
        consumer_key=local_settings.CONSUMER_KEY,
        consumer_secret=local_settings.CONSUMER_SECRET,
        access_token_key=local_settings.ACCESS_TOKEN_KEY,
        access_token_secret=local_settings.ACCESS_TOKEN_SECRET)

    kapi = krakenex.API()
    kapi.load_key(KRAKEN_AUTH_PATH)

    state = load_state(STATE_PATH)
    if state is None:
        state = {}

    cur_tweet_id = 0
    while True:
        try:
            state, cur_tweet_id = vicki_refresh_pos(tapi, state, cur_tweet_id)
        except twitter.error.TwitterError as tex:
            log.error("Twitter authenticated failed: %s "
                      "Please ensure that credentials are properly set in "
                      "local_settings.py", tex)
            sys.exit(1)

        state = kraken_refresh_pos(state, kapi)
        state = trading_state_machine(state, kapi)

        log.info('Current state: %s', state)
        save_state(STATE_PATH, state)
        time.sleep(WAIT_SECONDS)


if __name__ == '__main__':
    main()
