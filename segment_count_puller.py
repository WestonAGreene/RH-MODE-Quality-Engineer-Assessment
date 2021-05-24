"""
Pulls the current count from a segment.

USXXXXX
changeReport.ContactUploadProcess.ReduceTheInefficientTimeSpentByVeritcurl.Wgreene.2018-01-08
Created 2017-01-26 20:57
Modified 2018-02-20 16:32


Improvements:
- Monitor time to refresh for segments
- Create separate alert for count of segments that failed to refresh

"""
import json
import os
import datetime
import time
import sys
import pytz
import requests
from pyeloqua import Eloqua
from pprint import pprint
from visor_functions import VisorFunctions

VISOR_FUNCTIONS = VisorFunctions()

SEG_RECAL_WAIT_TIME = 15  # minutes
SEG_RECAL_MAX_TIME = 0


SEGMENTS_TO_COUNT = {
    "75320": {'segment_name': 'Global Unsub n Crtd Last Hr', 'monitor': 'weekly'},
    "75319": {'segment_name': 'Global Unsub n Not Crtd Last Hr', 'monitor': 'weekly'},
    "75318": {'segment_name': 'Global Sub n Not Crtd Last Hr', 'monitor': 'weekly'},
    "75317": {'segment_name': 'Global Sub n Crtd Last Hr', 'monitor': 'weekly'},
}


def visor_eloqua_segment():
    """Combines all functions"""

    print(
        "Starting script visor_eloqua_segment\n" +
        "This script will: \n" +
        "for a list of Eloqua segments, refresh them and record their new counts." +
        "\n\n\n",
    )

    global SEG_RECAL_MAX_TIME
    SEG_RECAL_MAX_TIME = datetime.datetime.now(
    ) + datetime.timedelta(minutes=SEG_RECAL_WAIT_TIME)

    elq = VISOR_FUNCTIONS.login_elq()

    for key, value in SEGMENTS_TO_COUNT.items():
        if 'monitor' not in value:
            value['monitor'] = "daily"

    for key, value in SEGMENTS_TO_COUNT.items():
        if 'custom_label' not in value:
            value['custom_label'] = None

    # Trigger refresh
    for key, value in SEGMENTS_TO_COUNT.items():
        value['time_previous_calc'] = segment_refresh(
            segment_id=key, elq_auth=elq)
        # small wait to prevent overloading Eloqua  # I have no categorical evidence to support this; merely gut.
        time.sleep(1)

    # Pull counts
    for key, value in SEGMENTS_TO_COUNT.items():
        value['count'] = segment_get_count(
            segment_id=key, time_previous_calc=value['time_previous_calc'], elq_auth=elq)


    # metrics for prometheus
    metric_desc = "Count returned by an Eloqua Segment."
    metric_list = []
    for key, value in SEGMENTS_TO_COUNT.items():
        if value['count'] >= 0:
            metric_list.append({
                'metric_name': value['segment_name'],
                'metric_desc': metric_desc,
                'metric_value': value['count'],
                'monitor': value['monitor'],
                'custom_label': value['custom_label']
            })
    return metric_list


def segment_refresh(
        segment_id,
        elq_auth,
        elq_api_endpoint="https://secure.p01.eloqua.com/api/rest/2.0/assets/contact/segment/",
):
    """
    Trigger a refresh of the segment
    """

    # Get segment current state
    segment_url = '%s%s' % (elq_api_endpoint, segment_id)
    get_response = requests.get(
        url='%s/count' % segment_url,
        auth=elq_auth.auth,
    ).json()
    time_previous_calc = get_response['lastCalculatedAt']

    # Tell segment to refresh
    VISOR_FUNCTIONS.retry(
        function=requests.post,
        kwargs={
            'url': '%squeue/%s' % (elq_api_endpoint, segment_id),
            'auth': elq_auth.auth,
        },
        print_kwargs=False,
    )

    print(
        "Triggered refresh for segment_id " + segment_id, 0)

    return time_previous_calc


def segment_get_count(
        segment_id,
        time_previous_calc,
        elq_auth,
        elq_api_endpoint="https://secure.p01.eloqua.com/api/rest/2.0/assets/contact/segment/",
):
    """
    Pull the segments count when the refresh finishes
    """

    # Get segment's new state
    segment_url = '%s%s' % (elq_api_endpoint, segment_id)
    get_response = requests.get(
        url='%s/count' % segment_url,
        auth=elq_auth.auth,
    ).json()
    seg_last_calculated = get_response['lastCalculatedAt']
    print(
        '\ntime_previous_calc' + time_previous_calc +
        '\nseg_last_calculated' + seg_last_calculated +
        "\nsegment_url = " + segment_url
    )

    error_message = None
    while_count = 0
    print('\nWaiting for segment to recalculate.')
    waiting = True
    while waiting:
        attempts = 1
        requesting = True
        while requesting:
            get_response = VISOR_FUNCTIONS.retry(
                function=(
                    lambda **kwargs: requests.get(**kwargs).json()
                ),
                kwargs={
                    'url': '%s/count' % segment_url,
                    'auth': elq_auth.auth,
                },
                print_kwargs=False,
            )
            seg_last_calculated = get_response['lastCalculatedAt']
            if seg_last_calculated != time_previous_calc:
                waiting = False
            elif datetime.datetime.now() > SEG_RECAL_MAX_TIME:
                error_message = ('Segment {segment_id} exceeded {SEG_RECAL_WAIT_TIME} minutes to recalculate'.format(
                    segment_id=segment_id, SEG_RECAL_WAIT_TIME=SEG_RECAL_WAIT_TIME))
                print(
                    error_message, print_regardless=True)
                waiting = False
            else:
                print(
                    'Waiting an additional 10 seconds for segment:' + str(segment_id))
                time.sleep(10)
                while_count += 1
                # if (while_count * 10) > (SEG_RECAL_WAIT_TIME * 60):
            requesting = False

    if error_message:
        segment_count = -1
    else:
        segment_count = int(get_response['count'])

    return segment_count



if __name__ == '__main__':

    visor_eloqua_segment()
