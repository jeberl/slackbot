#!/usr/bin/env python
from multiprocessing import Pool
from collections import defaultdict
from datetime import datetime
import shelve
import json
import os

from cachetools import LRUCache, cached

import requests
from flask import Flask, request, Response
from pidfile import PIDFile

import BigQuery
import BIGQUERY_PROJECTID

SLACK_WEBHOOK_SECRET = os.environ.get('SLACK_WEBHOOK_SECRET')
PD_API_KEY = os.environ.get('PD_API_KEY')
PD_NAME = os.environ.get('PD_NAME')
BIGQUERY_EVENTS_DATASET = os.environ.get('BIGQUERY_EVENTS_DATASET', "test_dataset")
BIGQUERY_EVENTS_TABLE = os.environ.get('BIGQUERY_EVENTS_TABLE', "test_table")

DEFAULT_ONLINE_LIMIT = 2
DEFAULT_TIME_SPAN = 'MINUTE'
DEFAULT_STAT_LIMIT = 5


HEADERS = {'Content-Type': 'application/json',
           'Accept': 'application/vnd.pagerduty+json;version=2',
           'Authorization': 'Token token={token}'.format(token=PD_API_KEY)}

COLORS = ["#f49d41", "#d6f441", '#41f449', '#41f4d3', '#4188f4', '#be41f4', '#f4415b']

cache = LRUCache(maxsize=30)

app = Flask(__name__)

def headers_filter(headers_list):
    return {key: value for key, value in HEADERS.iteritems() if key in headers_list}

def list_oncalls():
    oncall_url = 'https://api.pagerduty.com/oncalls'
    response = requests.get(oncall_url, headers=headers_filter(['Accept', 'Authorization']))
    oncalls = response.json()['oncalls']
    duties = {"Operations Schedule": None, "Platform Support": None, "Platform Support 2": None}
    contact_methods = defaultdict(dict)
    for oncall in oncalls:
        if oncall['end'] and oncall['start']:
            contact_id = oncall['user']['id']
            try:
                duties[oncall['schedule']['summary']] = (oncall['user']['summary'], contact_id)
                for method in get_contact_json(contact_id):
                    contact_methods[method['type']][oncall['user']['summary']] = method['address']
            except AttributeError:
                pass
    if all(duties.values()):
        return oncall_response_format(duties, contact_methods)
    return "No one on call"

@cached(cache)
def get_contact_json(contact_id):
    contact_url = 'https://{0}.pagerduty.com/api/v1/users/{1}/contact_methods'.format(PD_NAME, contact_id)
    return requests.get(contact_url, headers=headers_filter(['Content-type', 'Authorization'])).json().get('contact_methods', [])

def help_command():
    resp = "The commands you can use are: "
    for command in COMMANDS:
        resp += "* " + command['keys'][0] + "*, "
    return resp[0: len(resp) - 2]

def online(text):
    param = get_first_int(text, DEFAULT_ONLINE_LIMIT)
    timespans = ["SECOND", "MINUTE", "HOUR", "DAY"]
    timespan = DEFAULT_TIME_SPAN
    for span in timespans:
        if span in text.upper():
            timespan = span
            break
    bqclient = BigQuery(BIGQUERY_PROJECTID, BIGQUERY_EVENTS_DATASET)

    query = """SELECT json_extract_scalar(event_data, '$.user_name') as user_name,
                json_extract_scalar(event_data, '$.user_account_id') as user_account_id,
                json_extract_scalar(event_data, '$.browser') as browser,
                count(json_extract_scalar(event_data, '$.user_name')),
                max(event_ts) as last_activity,
                FROM {dataset}.{table}
                where 1=1
                and event_ts >= date_add(current_timestamp(), -{param}, "{timespan}")
                group by user_name, browser, user_account_id,""".format(dataset=BIGQUERY_EVENTS_DATASET,
                                                                        table=BIGQUERY_EVENTS_TABLE,
                                                                        param=param, timespan=timespan)
    events = bqclient.query(query)
    if not events:
        return "There are no active users in the past {} {}s".format(param, timespan.lower())
    if events[0].get("errors", None):
        return "Query Error: {}".format(events["errorResult"]["message"])

    def getinfo(event):
        info = [info_type['v'] for info_type in event['f']]
        if info[0]:
            return tuple(info)
        return None
    return online_response_format(param, timespan.lower(), [getinfo(event) for event in events])

def display_stats(text):
    users = shelve.open('./storage/user.dict')
    if not users.keys():
        return "No usage data to report"
    param = get_first_int(text, DEFAULT_STAT_LIMIT)
    response = {"text" : "Slackbot Champions:", "attachments":[]}

    rank = 1
    for name, calls in sorted(users.iteritems(), key=lambda (k, v): (v, k), reverse=True):
        if rank <= param:
            response["attachments"].append(stat_response_format(name, calls, rank))
            rank += 1
    users.close()
    return response


def stat_response_format(name, calls, rank):
    return {"title": "{}. {}".format(rank, name),
            "text": "{} Slackbot queries".format(calls),
            "color": "{}".format(COLORS[rank % len(COLORS)])
           }

def get_first_int(text, default):
    preceding_words = text.split(" ")
    count = len(preceding_words)
    while count > 0:
        count -= 1
        try:
            return int(preceding_words[count])
        except ValueError:
            pass
    return default

def reset_stats():
    for item in COMMANDS:
        item['calls'] = 0
    users = shelve.open('./storage/user.dict')
    users.clear()
    users.close()
    return "Usage Statistics cleared"

COMMANDS = [
    {'keys': ["help", "list"],
     'params': False,
     'function': help_command,
     'calls' : 0},
    {'keys': ["oncall", "on call", "pager duty", "PagerDuty"],
     'params': False,
     'function': list_oncalls,
     'calls' : 0},
    {'keys': ["online", "on line"],
     'params': True,
     'function': online,
     'calls' : 0},
    {'keys': ["stat", "usage"],
     'params': True,
     'function': display_stats,
     'calls' : 0},
    {'keys': ["reset"],
     'params': False,
     'function': reset_stats,
     'calls' : 0}
]

@app.route('/ping', methods=['GET'])
def ping():
    return str({'now': str(datetime.now())})


@app.route('/', methods=['POST'])
def index():
    users = shelve.open('./storage/user.dict')
    url = request.form.get('response_url')
    if request.form.get('payload', None):
        button_interaction(json.loads(request.form.get('payload')))
        return Response(), 200
    pool = Pool(processes=1)
    text = request.form.get('text')
    token = request.form.get('token')
    user_name = str(request.form.get('user_name'))

    if "secret" not in text:
        if users.has_key(user_name):
            num_calls = users[user_name]
            users[user_name] = num_calls + 1
        else:
            users[user_name] = 1
    users.close()
    pool.apply_async(respond, args=(url, text, token))
    requests.post(url, data=json.dumps({"text": "Ok. One second..."}), headers=headers_filter(['Content-type']))
    pool.close()
    pool.join()
    return Response(), 200

def button_interaction(payload):
    url = payload.get('response_url')
    action = payload.get("actions")[0].get("name")
    if action == 'reset':
        reset_stats()
    if action == 'contact_type':
        data = payload["actions"][0]["selected_options"][0]['value'].split("-")
        requested_method = data[0]
        contact_values = json.loads(data[1])
        response = {"text": "{}s:".format(requested_method.capitalize()), "attachments" : [{"text" : ""}]}
        for name, value in contact_values.iteritems():
            response["attachments"][0]["text"] += "{} - {}\n".format(name, value)
        requests.post(url, data=json.dumps(response), headers=headers_filter(['Content-type']))


def respond(url, text, token):
    response = None
    if token != SLACK_WEBHOOK_SECRET:
        return 'Incorrect token'
    for command in COMMANDS:
        for key in command['keys']:
            if key in text:
                if command['params']:
                    argument = text.split(key)[1]
                    response = command['function'](argument)
                else:
                    response = command['function']()
                command['calls'] += 1
    if not response:
        response = "Not sure what you mean \n" + help_command()
    if not isinstance(response, dict):
        response = {'text': response}
    requests.post(url, data=json.dumps(response), headers=headers_filter(['Content-type']))

def oncall_response_format(users, contact_methods):
    return {
        "attachments": [
            {
                "title": "Ops Schedule",
                "fields": [
                    {
                        "title": "On Call",
                        "value": "{}".format(users["Ops Schedule"][0]),
                        "short": True
                    },
                    {
                        "title": "Backup",
                        "value": "{}".format(users["Platfrom Support"][0]),
                        "short": True
                    }
                ],
                "color": COLORS[0]
            },
            {
                "title": "Platform Support",
                "fields": [
                    {
                        "title": "On Call",
                        "value": "{}".format(users["Platfrom Support"][0]),
                        "short": True
                    },
                    {
                        "title": "Backup",
                        "value": "{}".format(users["Platfrom Support 2"][0]),
                        "short": True
                    }
                ],
                "color": COLORS[1]
            },
            {
                "title": "Would you like to see contact information?",
                "attachment_type": "default",
                "callback_id": "contact_type",
                "actions": [{"name": "contact_type",
                             "type": "select",
                             "options": [{"text" : contact_method.capitalize(),
                                          "value": contact_method + "-" + str(contact_values).replace("u'", '"').replace("'", '"')}
                                         for contact_method, contact_values in contact_methods.iteritems()]
                            }],
                "color": COLORS[2]
            }
        ]
    }


def online_response_format(minutes, span, users):
    active_users = 0
    message_json = {"attachments": []}

    def userformat(name, account, operating_system, actions, time):
        return {"text": "*{}* - Account: {}".format(name, account),
                "fields": [
                    {
                        "value": "*{}* actions in past {} {}s".format(str(actions), minutes, span),
                        "short": True
                    },
                    {
                        "value": "<!date^{0}^Last Action at: {time}|Last Action at backup: {1}>".format(
                            int(float(time)), datetime.fromtimestamp(int(float(time))).strftime("%H : %M"), time='{time}'),
                        "short": True
                    },
                    {
                        "value": "Operating System: {}".format(operating_system),
                        "short": True
                    }
                ],
                "mrkdwn_in": ["text", "fields"],
                "color": COLORS[active_users % len(COLORS)]
               }

    for user in users:
        if user:
            message_json['attachments'].append(userformat(*user))
            active_users += 1

    message_json["text"] = "*{}* users have been active in the past {} {}s".format(active_users, str(minutes), span)
    return message_json

if __name__ == "__main__":
    with PIDFile('./storage/slackbot.pid'):
        app.run(host='0.0.0.0', port=5341)
