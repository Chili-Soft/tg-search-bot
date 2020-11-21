import os
import re
import requests
import json
from datetime import datetime

from pyquery import PyQuery as pq

EXPORT_DIR = 'Export'
MESSAGE_FILENAME_PATTERN = 'messages[0-9]*\.html'

message_files = [
    os.path.join(EXPORT_DIR, filename)
    for filename in os.listdir(EXPORT_DIR)
    if re.match(MESSAGE_FILENAME_PATTERN, filename)
]

def parse_message_file(filename):
    with open(filename, 'r') as f:
        content = f.read()
    page = pq(content)
    history = page('div.history')
    messages_element = [
        child for child in history.children()
        if re.match('message[0-9]+', child.get('id', ''))
    ]
    messages = []
    for msg in messages_element:
        msg = pq(msg)
        if not msg('div.text').text():
            continue
        ts = datetime.strptime(msg('div.date').attr('title'), '%d.%m.%Y %H:%M:%S')
        messages.append({
            'msg': msg('div.text').text(),
            'user': msg('div.from_name').text(),
            'timestamp': ts.strftime('%s'),
            'msg_id': re.findall('message([0-9]+)', msg.attr('id'))[0],
        })
    return messages


def add_redisearch(message):
    idx = int(message['msg_id'])
    url = 'http://127.0.0.1:12321/add'
    chat_id = ''    # use a real chat_id
    params = {
        'msg': message['msg'],
        'user': message['user'],
        'ts': message['timestamp'],
        'msg_id': idx,
        'chat_id': chat_id,
    }
    r = requests.get(url, params)
    print(idx, r.status_code, r.content)


for msg_file in message_files:
    for msg in parse_message_file(msg_file):
        add_redisearch(msg)

