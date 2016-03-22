#!/usr/bin/env python

import requests
from requests_oauthlib import OAuth1
# import oauth2 as oauth
import yaml
# import urllib
import json
import os
import time
# import pytz
import datetime
import argparse
import re
import sys


DOMAIN = 'https://ads-api.twitter.com'
VERBOSE = 0
NON_SUB_PARAM_SEGMENTATION_TYPES = ['PLATFORMS', 'LOCATIONS', 'GENDER', 'INTERESTS', 'KEYWORDS']
DEFAULT_METRIC_GROUPS = ['BILLING', 'ENGAGEMENT', 'VIDEO', 'MEDIA']
DEFAULT_PLACEMENT = 'ALL_ON_TWITTER'


def main(options):
    global VERBOSE
    account = options.account_id
    headers = options.headers
    if options.veryverbose:
        VERBOSE = 2
    elif options.verbose:
        VERBOSE = 1
    start = time.clock()
    user_twurl = twurlauth()

    print("Best practices stats check for :account_id %s" % account)
    linesep()

    now = datetime.datetime.utcnow()
    start_time = datetime.datetime.utcnow() - datetime.timedelta(days=60)
    start_time = start_time.replace(minute=0, second=0, microsecond=0)
    end_time = datetime.datetime.utcnow()
    end_time = end_time.replace(minute=0, second=0, microsecond=0)
    print('Current time:\t%s' % now)
    print('Start time:\t%s' % start_time)
    print('End time:\t%s' % end_time)
    linesep()

    # check that we have access to this :account_id
    resource_path = '/1/accounts/%s' % account
    data = get_data(user_twurl, 'GET', headers, DOMAIN + resource_path)

    if len(data) == 0:
        print('ERROR: Could not locate :account_id %s' % account)
        sys.exit(0)

    # fetch funding instruments
    resource_path = '/1/accounts/%s/funding_instruments?with_deleted=true&count=1000' % account
    data = get_data(user_twurl, 'GET', headers, DOMAIN + resource_path)

    # filter funding instruments
    print("Pre-filtered data:\t\t%s" % len(data))
    funding_instruments = check(data, start_time, end_time)
    print("Funding instruments:\t\t%s" % len(funding_instruments))

    # fetch campaigns
    resource_path = '/1/accounts/%s/campaigns?with_deleted=true&count=1000' % account
    data = get_data(user_twurl, 'GET', headers, DOMAIN + resource_path)

    # filter campaigns
    print("Pre-filtered data:\t\t%s" % len(data))
    campaigns = check(data, start_time, end_time, 'funding_instrument_id', funding_instruments)
    print("Campaigns:\t\t\t%s" % len(campaigns))

    # fetch line items
    resource_path = '/1/accounts/%s/line_items?with_deleted=true&count=1000' % account
    data = get_data(user_twurl, 'GET', headers, DOMAIN + resource_path)

    # filter line items
    print("Pre-filtered data:\t\t%s" % len(data))
    line_items = check(data, start_time, end_time, 'campaign_id', campaigns)
    print("Line items:\t\t\t%s" % len(line_items))

    # fetch promoted_tweets
    resource_path = '/1/accounts/%s/promoted_tweets?with_deleted=true&count=1000' % account
    data = get_data(user_twurl, 'GET', headers, DOMAIN + resource_path)

    # filter promoted_tweets
    print("Pre-filtered data:\t\t%s" % len(data))
    promoted_tweets = check(data, start_time, end_time, 'line_item_id', line_items)
    print("Promoted Tweets:\t\t%s" % len(promoted_tweets))

    total_query_count = 0
    total_request_cost = 0
    total_rate_limited_query_count = 0
    segmented_query_count = 0
    segmented_request_cost = 0

    if len(line_items) > 0:
        print("\tfetching stats for %s line items" % len(line_items))
        (query_count,
         cost_total,
         rate_limited_query_count) = gather_stats(user_twurl, headers, account, 'LINE_ITEM',
                                                  start_time, end_time, line_items)

        total_query_count += query_count
        total_request_cost += cost_total

    if len(promoted_tweets) > 0:
        print("\tfetching stats for %s promoted tweets" % len(promoted_tweets))
        (query_count,
         cost_total,
         rate_limited_query_count) = gather_stats(user_twurl, headers, account, 'PROMOTED_TWEET',
                                                  start_time, end_time, promoted_tweets)

        total_query_count += query_count
        total_request_cost += cost_total
        total_rate_limited_query_count += rate_limited_query_count

    # Segmentation queries
    if options.segmentation:
        if len(line_items) > 0:
            print("\tfetching segmentation stats for %s line items" % len(line_items))
            for i in NON_SUB_PARAM_SEGMENTATION_TYPES:
                (query_count,
                 cost_total,
                 rate_limited_query_count) = gather_stats(user_twurl, headers, account,
                                                          'line_items', start_time, end_time,
                                                          line_items, i)

                total_query_count += query_count
                total_request_cost += cost_total
                segmented_query_count += query_count
                segmented_request_cost += cost_total

        if len(promoted_tweets) > 0:
            print("\tfetching segmentation stats for %s promoted tweets" % len(promoted_tweets))
            for i in NON_SUB_PARAM_SEGMENTATION_TYPES:
                (query_count,
                 cost_total,
                 rate_limited_query_count) = gather_stats(user_twurl, headers, account,
                                                          'promoted_tweets', start_time, end_time,
                                                          promoted_tweets, i)

                total_query_count += query_count
                total_request_cost += cost_total
                segmented_query_count += query_count
                segmented_request_cost += cost_total

    linesep()
    if options.segmentation:
        print("Non-Seg Stats Req Cost:\t\t%s" % (total_request_cost - segmented_request_cost))
        print("Segmented Stats Req Cost:\t%s" % segmented_request_cost)
        linesep()
    print("Total Stats Queries:\t\t%s" % total_query_count)
    print("Total Stats Request Cost:\t%s" % total_request_cost)
    if VERBOSE > 0:
        print("Avg Cost per Query:\t\t%s" % str(total_request_cost / total_query_count))
    print("Queries Rate Limited:\t\t%s" % total_rate_limited_query_count)
    linesep()

    elapsed = (time.clock() - start)
    print('Time elapsed:\t\t\t%s' % elapsed)


def input():
    p = argparse.ArgumentParser(description='Fetch Twitter Ads Account Stats')

    p.add_argument('-a', '--account', required=True, dest='account_id', help='Ads Account ID')
    p.add_argument('-A', '--header', dest='headers', action='append',
                   help='HTTP headers to include')
    p.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                   help='Verbose outputs cost avgs')
    p.add_argument('-vv', '--very-verbose', dest='veryverbose', action='store_true',
                   help='Very verbose outputs API queries made')
    p.add_argument('-s', '--segmentation', dest='segmentation', help='Pull segmentation stats',
                   action='store_true')
    p.add_argument('-m', '--metric-groups', dest='metric_groups', action='store_true',
                   help='Metric groups to fetch. Default to %s' % ','.join(DEFAULT_METRIC_GROUPS))
    p.add_argument('-p', '--placement', dest='placement', help='Placement to fetch. Defaults to %s' % DEFAULT_PLACEMENT,
                   action='store_true')
    args = p.parse_args()

    return args


def twurlauth():
    with open(os.path.expanduser('~/.twurlrc'), 'r') as f:
        contents = yaml.load(f)
        f.close()

    default_user = contents["configuration"]["default_profile"][0]

    CONSUMER_KEY = contents["configuration"]["default_profile"][1]
    CONSUMER_SECRET = contents["profiles"][default_user][CONSUMER_KEY]["consumer_secret"]
    USER_OAUTH_TOKEN = contents["profiles"][default_user][CONSUMER_KEY]["token"]
    USER_OAUTH_TOKEN_SECRET = contents["profiles"][default_user][CONSUMER_KEY]["secret"]

    return CONSUMER_KEY, CONSUMER_SECRET, USER_OAUTH_TOKEN, USER_OAUTH_TOKEN_SECRET


def request(user_twurl, http_method, headers, url, param_data=None):
    CONSUMER_KEY = user_twurl[0]
    CONSUMER_SECRET = user_twurl[1]
    USER_OAUTH_TOKEN = user_twurl[2]
    USER_OAUTH_TOKEN_SECRET = user_twurl[3]

    auth = OAuth1(CONSUMER_KEY, CONSUMER_SECRET, USER_OAUTH_TOKEN, USER_OAUTH_TOKEN_SECRET)

    header_list = {}
    if headers:
        for i in headers:
            (key, value) = i.split(': ')
            if key and value:
                header_list[key] = value

    response = requests.request(http_method, url, auth=auth, headers=header_list, data=param_data)

    try:
        data = response.json()
    except:
        data = None
    return response.status_code, response.headers, data


def get_data(user_twurl, http_method, headers, url):
    data = []

    status_code, res_headers, response = request(user_twurl, http_method, headers, url)

    if status_code != 200:
        print('ERROR: query failed, cannot continue: %s' % url)
        sys.exit(0)

    if response and 'data' in response:
        data += response['data']

    while 'next_cursor' in response and response['next_cursor'] is not None:
        cursor_url = url + '&cursor=%s' % response['next_cursor']
        status_code, res_headers, response = request(user_twurl, http_method, headers, cursor_url)

        if response and 'data' in response:
            data += response['data']

    return data


def gather_stats(user_twurl, headers, account_id, entity_type, start_time, end_time, input_entities,
                 segmentation=None):

    entities = list(input_entities)
    resource_url = DOMAIN + "/1/stats/jobs/accounts/%s" % account_id

    param_data = {
        'entity': entity_type,
        'granularity': 'HOUR',
        'start_time': start_time.isoformat()+'Z',
        'end_time': end_time.isoformat()+'Z',
        'placement': DEFAULT_PLACEMENT,
        'metric_groups': ','.join(DEFAULT_METRIC_GROUPS)
    }

    if segmentation:
        param_data['segmentation_type'] = segmentation

    query_count = 0
    cost_total = 0
    rate_limited_query_count = 0
    limit_exceeded_sleep = 0

    while entities:
        if limit_exceeded_sleep > 0:
            print('\t! sleeping for %s' % limit_exceeded_sleep)
            time.sleep(limit_exceeded_sleep)
            limit_exceeded_sleep = 0

        query_entities = []
        limit = 20
        if len(entities) < limit:
            limit = len(entities)

        for _ in range(limit):
            query_entities.append(entities.pop(0))

        param_data['entity_ids'] = ','.join(query_entities)

        status_code, res_headers, res_data = request(user_twurl, 'POST', headers, resource_url, param_data)

        if 'x-request-cost' in res_headers:
            cost_total += int(res_headers['x-request-cost'])
            reset_at = int(res_headers['x-cost-rate-limit-reset'])

            if (('x-cost-rate-limit-remaining' in res_headers and
                    int(res_headers['x-cost-rate-limit-remaining']) == 0) and
                    status_code == '429'):
                limit_exceeded_sleep = reset_at - int(time.time())

        if status_code == 200:
            query_count += 1
            job_id = res_data['data']['id']
            check_status(user_twurl, headers, account_id, job_id)
            if VERBOSE > 1:
                print('VERBOSE:\tStats Query:\t%s' % stats_url)
        elif status_code == 429:
            print("RATE LIMITED! adding entities back to queue")
            rate_limited_query_count += 1
            entities.extend(query_entities)
        elif status_code == 503:
            print("TIMEOUT!")
            print(stats_url)
            entities.extend(query_entities)
        else:
            print("ERROR %s" % status_code)
            print(res_headers)
            print(res_data)
            sys.exit(0)

    if VERBOSE > 0:
        if segmentation:
            print('VERBOSE:\tSegmentation type:\t%s' % segmentation)

    return query_count, cost_total, rate_limited_query_count


def check_status(user_twurl, headers, account_id, job_id):
    SLEEP_TIME = 15

    resource_url = DOMAIN + "/1/stats/jobs/accounts/%s?job_ids=%s" % (account_id, job_id)

    print resource_url

    status_code, res_headers, res_data = request(user_twurl, 'GET', headers, resource_url)

    if status_code == 200:
        res_data = res_data['data'][0]
        if res_data['status'] == 'SUCCESS':
            print('Job ID %s Completed.' % job_id)
            if VERBOSE > 0:
                print('VERBOSE: Job URL: %s' % data['url'])
        elif res_data['status'] == 'FAILED':
            print('Job ID %s FAILED.' % job_id)
        else:
            print("Waiting for job id %s to complete" % job_id)
            time.sleep(SLEEP_TIME)
            check_status(user_twurl, headers, account_id, job_id)
    elif status_code == 429:
        print("RATE LIMITED! Waiting to requery status")
        if 'x-rate-limit-reset' in res_headers:
            reset_at = int(res_headers['x-rate-limit-reset'])
            time.sleep(reset_at - int(time.time()))
    elif status_code == 503:
        print("TIMEOUT! Waiting to requery status")
        if 'x-retry-after' in res_headers:
            print("TIMEOUT! Waiting to requery status")
            retry_after = int(res_headers['x-rate-limit-reset'])
            time.sleep(retry_after - int(time.time()))
        else:
            print("TIMEOUT!")
            print(res_headers)
            print(res_data)
            sys.exit(0)
    else:
        print("ERROR %s" % status_code)
        print(res_headers)
        print(res_data)
        sys.exit(0)

    if VERBOSE > 0:
        if segmentation:
            print('VERBOSE:\tSegmentation type:\t%s' % segmentation)


def check(data, start_time, end_time, filter_field=None, filter_data=[]):

    d = []

    if data and len(data) > 0:
        for i in data:
            if 'end_time' in i and i['end_time'] and format_timestamp(i['end_time']) < start_time:
                continue
            elif ('start_time' in i and i['start_time'] and
                  format_timestamp(i['start_time']) > end_time):
                continue
            elif i['deleted'] and format_timestamp(i['updated_at']) < start_time:
                continue
            elif i['paused'] and format_timestamp(i['updated_at']) < start_time:
                continue
            elif filter_field and i[filter_field] not in filter_data:
                continue
            else:
                d.append(i['id'])

    return d


def format_timestamp(timestamp):
    return datetime.datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')


def linesep():
    print('-----------------------------------------------')


if __name__ == '__main__':
    options = input()
    main(options)
