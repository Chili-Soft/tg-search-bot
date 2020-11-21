# coding: utf8
import requests
import json
import sys
import os
import logging
import socket
import psutil
import time
import platform
from datetime import datetime

import telegram
from telegram import InlineKeyboardButton
from telegram import InlineKeyboardMarkup
from telegram.ext import CommandHandler, Updater, filters, MessageHandler, CallbackQueryHandler
from telegram.ext.dispatcher import run_async

from config import BOT_TOKEN as TOKEN
from config import OWNER_ID
from config import ENABLED_CHATS

logger = logging.getLogger('bot')
logger.setLevel(level=os.environ.get("LOGLEVEL") or logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    '[%(levelname)s][%(asctime)s][%(name)s.%(funcName)s:%(lineno)d] %(message)s'
)
handler.setFormatter(formatter)
logger.addHandler(handler)

SERVER_STATS_RESPONSE = '''
*OS:* `{os_name}`

*CPU:*
    - `user: {user:0.2f} %`
    - `sys.: {system:0.2f} %`
    - `idle: {idle:0.2f} %`

*Memory:*
    - `total: {total:0.2f} MB`
    - `used:  {used:0.2f} MB`

*Network:*
    - `sent: {sent:0.2f} KB/s`
    - `recv: {recv:0.2f} KB/s`
'''

SEARCH_RESPONSE_ITEM = u'''[{time} - {user}]({url}): {text}

'''

SEARCH_RESPONSE = u'''"*{term}*" 的搜索结果: (第 {page} 页)
{body}
'''


@run_async
def os_stats(update, context):
    logger.info('command: [os_stats] started')
    cpu = psutil.cpu_times_percent(interval=0.5)
    memory = psutil.virtual_memory()
    network_io = psutil.net_io_counters()
    time.sleep(1)
    network_io_new = psutil.net_io_counters()
    net_send = network_io_new.bytes_sent - network_io.bytes_sent
    net_recv = network_io_new.bytes_recv - network_io.bytes_recv
    msg = SERVER_STATS_RESPONSE.format(
        os_name=platform.platform(),
        user=cpu.user, system=cpu.system, idle=cpu.idle,
        total=memory.total / 1024. / 1024., used=memory.used / 1024. / 1024.,
        sent=net_send / 1024., recv=net_recv / 1024.
    )
    context.bot.send_message(
        chat_id=update.message.chat_id, text=msg, parse_mode='markdown'
    )
    logger.info('command: [os_stats] finished')


def do_search_cgi(chat_id, query, page=1):
    url = 'http://127.0.0.1:12321/search'
    params = {
        'chat_id': chat_id,
        'key': query,
        'page': page,
    }
    r = requests.get(url, params=params)
    return r.status_code, json.loads(r.content)


def do_add_doc_cgi(chat_id, msg_id, msg, user, ts):
    url = 'http://127.0.0.1:12321/add'
    params = {
        'chat_id': chat_id,
        'msg_id': msg_id,
        'msg': msg,
        'user': user,
        'ts': ts,
    }
    r = requests.get(url, params=params)
    return r.status_code, json.loads(r.content)


@run_async
def on_enable_search(update, context):
    logger.info('command [enable] started')
    message = update.message
    uid = message.from_user.id
    chat_id = message.chat_id
    if uid == OWNER_ID:
        logger.info('search enabled in %s' % chat_id)
        ENABLED_CHATS.add(chat_id)
        context.bot.send_message(
            chat_id=chat_id, text='search enabled in this chat'
        )
    # else:
    #     context.bot.send_message(chat_id=chat_id, text='not allowed')


@run_async
def on_search(update, context):
    logger.info('commamd: [search] started')
    bot = context.bot
    message = update.message
    if not message:
        logger.warn('command: [search] message empty; update: %s', update)
        return
    chat_id = message.chat_id
    msg_id = message.message_id
    if chat_id not in ENABLED_CHATS:
        return context.bot.send_message(
            chat_id=chat_id, text='not enabled'
        )
    query = ' '.join(message.text.split()[1:])
    if not query:
        context.bot.send_message(
            chat_id=chat_id, text='Usage: /search query')
        return
    logger.info('commamd: [search] searching: %s', query)
    code, result = do_search_cgi(chat_id, query)
    if code is not 200:
        context.bot.send_message(
            chat_id=chat_id,
            text='error: upstream responded with code %s' % code
        )
        return
    hits = result.get('hits', [])
    keyboard = get_paging_buttons(query)
    response_text = hits2response(query, hits)
    # logger.info('result: %s', response_text)
    context.bot.send_message(
        chat_id=chat_id,
        reply_to_message_id=msg_id,
        text=response_text,
        parse_mode='markdown',
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


@run_async
def on_search_paging_button(update, context):
    cb_query = update.callback_query
    chat_id = update.callback_query.message.chat_id
    msg_id = update.callback_query.message.message_id
    if is_del_button_data(cb_query.data):
        return on_del_search_result_button(update, context)

    page, query = parse_paging_button_data(cb_query.data)

    code, result = do_search_cgi(chat_id, query, page)
    if code is not 200:
        cb_query.edit_message_text(
            text='error: upstream responded with code %s' % code
        )
        return
    hits = result.get('hits', [])
    keyboard = get_paging_buttons(query, chat_id, msg_id, page)
    response_text = hits2response(query, hits, page)
    cb_query.edit_message_text(
        text=response_text,
        parse_mode='markdown',
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


def on_del_search_result_button(update, context):
    cb_query = update.callback_query
    chat_id, msg_id = parse_del_button_data(cb_query.data)
    if not chat_id or not msg_id:
        chat_id = update.callback_query.message.chat_id
        msg_id = update.callback_query.message.message_id
    logger.info(
        'delete search result chat_id = %s ; msg_id = %s', chat_id, msg_id,
    )
    context.bot.delete_message(chat_id=chat_id, message_id=msg_id)


def hits2response(query, hits, page=1):
    for hit in hits:
        hit['user'] = hit.get('user', '').replace('*', '\\*')
        hit['msg'] = hit.get('msg', '').replace('*', '\\*').replace('_', '\\_')
        ts = hit.get('ts')
        if not ts:
            hit['time'] = ''
        else:
            ts = datetime.fromtimestamp(float(ts))
            hit['time'] = ts.strftime('%Y/%m/%d %H:%M')
    items = [
        SEARCH_RESPONSE_ITEM.format(
            user=hit.get('user'), text=hit.get('msg'), url=hit.get('url'),
            time=hit.get('time'),
        )
        for hit in hits
    ]
    body = ''.join(items) or 'no result found'
    return SEARCH_RESPONSE.format(term=query, page=page, body=body)


def get_paging_buttons(query, chat_id='', msg_id='', current_page=1, more=True):
    buttons = []
    current_page = int(current_page)
    if current_page > 1:
        paging_data = make_paging_button_data(query, current_page - 1)
        buttons.append(
            InlineKeyboardButton("上一页", callback_data=paging_data)
        )
    if more:
        paging_data = make_paging_button_data(query, current_page + 1)
        buttons.append(
            InlineKeyboardButton("下一页", callback_data=paging_data)
        )
    del_data = make_del_button_data(chat_id, msg_id)
    del_button = [InlineKeyboardButton(u"❌ 关闭", callback_data=del_data)]
    return telegram.InlineKeyboardMarkup([
        buttons, del_button,
    ])


def make_paging_button_data(query, page):
    return u'{}:{}'.format(page, query)


def parse_paging_button_data(data):
    page, query = data.split(':', 1)
    return page, query


def make_del_button_data(chat_id, msg_id):
    return u'del:{}:{}'.format(chat_id, msg_id)


def is_del_button_data(data):
    return data.startswith('del:')


def parse_del_button_data(data):
    _, chat_id, msg_id = data.split(':')
    return chat_id, msg_id


@run_async
def on_new_message(update, context):
    message = update.message
    chat_id = message.chat_id
    msg_id = message.message_id
    if not message:
        logger.warn('message empty; update: %s', update)
        return

    # TODO indexing caption of image/files
    msg = message.text
    ts = message.date.strftime('%s')
    from_user = message.from_user   # TODO: get user's name in realtime
    user = from_user.first_name or ''
    user += ' ' + (from_user.last_name or '')
    user = user.strip()
    user = user or from_user.username or from_user.id
    if not msg or msg.startswith('/'):
        return

    code, result = do_add_doc_cgi(chat_id, msg_id, msg, user, ts)
    if code is not 200:
        logger.error(
            'upstream responded with code %s' % code
        )


CMD_CONTROLLER_MAP = {
    'os_stats': os_stats,
    'search': on_search,
    'enable': on_enable_search,
}


def main(token, request_kwargs=None):
    global CMD_CONTROLLER_MAP

    updater = Updater(token=token, request_kwargs=args, use_context=True)

    dispatcher = updater.dispatcher

    for cmd, func in CMD_CONTROLLER_MAP.items():
        dispatcher.add_handler(CommandHandler(cmd, func))

    dispatcher.add_handler(CallbackQueryHandler(on_search_paging_button))

    dispatcher.add_handler(MessageHandler(filters.Filters.all, on_new_message))

    updater.start_polling()
    updater.idle()


if __name__ == "__main__":
    args = {}
    if len(sys.argv) > 1 and sys.argv[1] == 'dev':
        logger.info('use proxy')
        args.update(proxy_url='socks5://127.0.0.1:10800')
    main(TOKEN, args)
