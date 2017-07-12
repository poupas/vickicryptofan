#!/usr/bin/env python

import time
import json
import re

from decimal import Decimal as D

import krakenex
import dateutil.parser

from twitter import Api

CONSUMER_KEY = ''
CONSUMER_SECRET = ''
ACCESS_TOKEN_KEY = ''
ACCESS_TOKEN_SECRET = ''

VICKI_USER = '@vickicryptobot'

WAIT_SECONDS = 120

PAIR_POSITIONS = None

STATE_PATH = 'state.dat'
KRAKEN_AUTH_PATH = 'kraken.auth'


PAIRS = {
    #'BTCUSD': {'buy': 200, 'kraken': 'XBTEUR', 'asset': 'XBT'},
    #'ETHBTC': {'kraken': 'ETHXBT', 'asset': 'ETH'},
    'ETHUSD': {
        'buy': D(500),
        'kraken_pair': 'ETHEUR',
        'kraken_asset': 'XETH',
        'asset': 'ETH'
    },
}

KRAKEN_PAIRS = {
    'XBTEUR': 'BTCUSD',
    'ETHEUR': 'ETHUSD',
}

REX = re.compile(
    '^I am\s+going\s+(?P<pos>short|long)(?:\s+on)?\s+(?P<pair>[A-Z]+)')


def kraken_handle_errors(func):
    def wrapper(*args, **kwargs):
        try:
            r = func(*args, **kwargs)
        except json.JSONDecodeError as jde:
            print(
                'Kraken request failed: %s. failed: %s, args: %s, kwargs: %s' %
                (jde, func, args, kwargs))
            return None

        except IOError as ioe:
            print(
                'Kraken request failed: %s. failed: %s, args: %s, kwargs: %s' %
                (ioe, func, args, kwargs))

        else:
            return r

    return wrapper


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
        with open(path, 'rb') as fp:
            return json.load(fp)
    except (ValueError, IOError, TypeError):
        return None


def save_state(path, state):
    with open(path, 'w', encoding='ascii') as fp:
        json.dump(state, fp)


def vicki_refresh_pos(api, state, cur_tweet_id):

    print('Fetching tweets...')
    timeline = api.GetUserTimeline(
        screen_name=VICKI_USER, exclude_replies=True, include_rts=False,
        trim_user=True, since_id=cur_tweet_id)

    for tweet in timeline:
        _id = int(tweet.id_str)
        cur_tweet_id = max(cur_tweet_id, _id)

        action = REX.match(tweet.text)
        if action is None:
            continue

        pair = action['pair']
        position = action['pos'].lower()
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


@kraken_handle_errors
def kraken_fetch_open_orders(api):
    response = api.query_private('OpenOrders', {})
    if response['error']:
        print('fetch_open_orders error: %s' % response['error'])
        return None

    try:
        open_orders = response['result']['open']
    except KeyError:
        return None

    orders = {}
    for txid, order in open_orders.items():
        pair = order['descr']['pair']
        pair = KRAKEN_PAIRS.get(pair, pair)
        otype = order['descr']['type']
        if pair not in orders:
            orders[pair] = []

        orders[pair].append({'txid': txid, 'type': otype})

    return orders


@kraken_handle_errors
def kraken_add_order(api, pair, _type, amount):

    args = {
        'pair': pair,
        'type': _type,
        'ordertype': 'market',
        'volume': amount,
    }
    response = api.query_private('AddOrder', args)
    if response['error']:
        print('add_order error: %s' % response['error'])
        return None

    print("Order %s was created." % response['result'])
    return response['result']


@kraken_handle_errors
def kraken_cancel_order(api, txid):
    response = api.query_private('CancelOrder', {'txid': txid})
    if response['error']:
        print('cancel_order error: %s' % response['error'])
        return False

    print("Order %s was cancelled." % txid)
    return True


@kraken_handle_errors
def kraken_fetch_balance(api):
    response = api.query_private('Balance', {})
    if response['error']:
        print('fetch_balance error: %s' % response['error'])
        return {}

    return(response['result'])


def kraken_fetch_asset_balance(api, asset):
    balance = kraken_fetch_balance(api)
    if balance is not None:
        return D(balance[asset])

    return None


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


@kraken_handle_errors
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
            print('Ignoring pair %s...' % pair)
            continue

        vicki = pair_state.get('vicki')
        if vicki is None:
            print('Vicki does not recognize %s as a valid pair.' % pair)
            continue

        assert(vicki['position'] in ('short', 'long'))

        kraken = pair_state.get('kraken')
        if kraken is not None:
            if kraken['position'] == vicki['position']:
                print('Kraken and Vicki synced. All good.')
                return state

            # Vicki and Kraken disagree. Clear open orders for this pair.
            txids = kraken.get('txids', ())
            for txid in txids:
                print('Removing txid %s from open orders' % txid)
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
            if to_sell is None:
                return state

            if to_sell > D('0.0001'):
                kraken_add_order(kapi, kraken_pair, 'sell', to_sell)
            kraken['position'] = 'short'
            kraken['txids'] = []

    return state


def main():
    tapi = Api(
        consumer_key=CONSUMER_KEY,
        consumer_secret=CONSUMER_SECRET,
        access_token_key=ACCESS_TOKEN_KEY,
        access_token_secret=ACCESS_TOKEN_SECRET)

    kapi = krakenex.API()
    kapi.load_key(KRAKEN_AUTH_PATH)

    state = load_state(STATE_PATH)
    if state is None:
        state = {}

    cur_tweet_id = 0
    while True:
        state, cur_tweet_id = vicki_refresh_pos(tapi, state, cur_tweet_id)
        state = kraken_refresh_pos(state, kapi)
        print('Current state: %s' % state)

        state = trading_state_machine(state, kapi)
        print('Current state: %s' % state)

        save_state(STATE_PATH, state)
        time.sleep(WAIT_SECONDS)


if __name__ == '__main__':
    main()
