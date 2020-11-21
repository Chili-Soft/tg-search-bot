# coding: utf-8
import logging
import json
import sys
import os

import flask
import requests
from flask import Flask
from flask import jsonify
from flask import request as flask_request

import redis
import redisearch

from config import PAGE_SIZE
from config import REDIS_HOST
from config import REDIS_PORT

logger = logging.getLogger('server')
logger.setLevel(level=os.environ.get("LOGLEVEL") or logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '[%(levelname)s][%(asctime)s][%(name)s.%(funcName)s:%(lineno)d] %(message)s'
)
handler.setFormatter(formatter)
logger.addHandler(handler)

app = Flask(__name__)

REDISEARCH_CLIS = {}


def get_index_name(chat_id):
    return 'chat_index_%s' % (chat_id,)


def get_doc_id(chat_id, msg_id):
    return 'doc_id_%s_%s' % (chat_id, msg_id)


def get_redisearch_cli(chat_id):
    idx = get_index_name(chat_id)
    # TODO supports for redis authentication & cluster
    cli = redisearch.Client(idx, host=REDIS_HOST, port=REDIS_PORT)
    logger.debug('get client with idx %s for chat %s', idx, chat_id)
    try:
        # cli.drop_index()  # TODO dedicate API for dropping index
        cli.create_index([
            redisearch.TextField('msg', weight=5.),
            redisearch.TextField('msg_id', weight=0.),
            redisearch.TextField('user', weight=0.),
            redisearch.TextField('ts', weight=0.),
        ])
    except redis.exceptions.ResponseError as e:
        if e.message != 'Index already exists':
            raise
    return cli


def do_search(chat_id, query, start=0, size=10):
    q = redisearch.Query(query)
    q.sort_by('ts', asc=False)
    q.paging(start, size)
    # q.summarize(fields=['msg'], context_len=24)
    q.language('chinese')

    cli = get_redisearch_cli(chat_id)
    res = cli.search(q)
    return res


def do_add(chat_id, msg_id, msg, user, ts):
    cli = get_redisearch_cli(chat_id)
    doc_id = get_doc_id(chat_id, msg_id)
    cli.add_document(
        doc_id,
        msg_id=msg_id,
        msg=msg,
        user=user,
        ts=ts,
        language='chinese',
    )


def safe_int(n, default=0):
    try:
        return int(n)
    except Exception:
        return default


def safe_str(s, default=''):
    if not s:
        return ''
    if isinstance(s, (str, unicode)):
        return s
    try:
        return str(s)
    except Exception:
        return default


@app.route('/add')
def http_add():
    ts = safe_str(flask_request.args.get('ts'))
    msg = safe_str(flask_request.args.get('msg'))
    msg_id = safe_str(flask_request.args.get('msg_id'))
    chat_id = safe_str(flask_request.args.get('chat_id'))
    user = safe_str(flask_request.args.get('user'))
    try:
        do_add(chat_id, msg_id, msg, user, ts)
        err_code = 0
        err_msg = ''
    except redis.exceptions.ResponseError as e:
        if e.message != 'Document already exists':
            err_code = 1
            err_msg = str(e)
        else:
            err_code, err_msg = 0, ''
    except Exception as e:
        err_code = 2
        err_msg = str(e)
    return jsonify({
        'err_code': err_code,
        'err_msg': err_msg,
    })


@app.route('/search')
def http_search():
    query = safe_str(flask_request.args.get('key'))
    chat_id = safe_str(flask_request.args.get('chat_id'))
    page = safe_int(flask_request.args.get('page'), default=1)
    page = 1 if page < 1 else page
    query = query.strip()
    start = (page - 1) * PAGE_SIZE
    size = PAGE_SIZE
    if query:
        try:
            result = do_search(chat_id, query, start, size)
            docs = result.docs
            total = result.total
            duration = result.duration
            hits = [
                {
                    'msg_id': item.msg_id,
                    'msg': item.msg,
                    'user': item.user,
                    'ts': item.ts,
                }
                for item in docs
            ]
            for item in hits:  # TODO move to bot module?
                if str(chat_id).startswith('-100'):
                    chat_url = 'c/' + chat_id[4:]
                else:
                    chat_url = 'c/' + chat_id
                item['url'] = 'https://t.me/%s/%s' % (chat_url, item['msg_id'])
            error = ''
        except Exception as e:
            error = str(e)
            hits = []
    else:
        hits = []
        error = ''
    if error:
        logger.error(
            'search [%s] in [%s] finished. error: %s', query, chat_id, error,
        )
    else:
        logger.info(
            'search [%s] in [%s] finished. found %s',
            query, chat_id, len(hits),
        )
    rtn = {
        'hits': hits,
        'error': error,
    }
    return jsonify(rtn)


app.run('127.0.0.1', 12321, debug=False)
