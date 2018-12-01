import json
import locale
import os
import pytz
import requests
import sendgrid

from csv import DictReader, DictWriter
from datetime import datetime, timedelta
from decimal import *
from pytz import timezone
from sendgrid.helpers.mail import *

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

def fetch_data():
    getcontext().prec = 10
    datetime_fmt_query = '%Y-%m-%dT00:00:00.0000'
    datetime_fmt_response = '%Y-%m-%dT%H:00:00'
    datetime_fmt_display = '%H:%M (%a, %m/%d)'
    from_tz = timezone('US/Eastern')
    to_tz = timezone('US/Central')
    fields = ['congestion_price_da', 'datetime_beginning_ept', 'pnode_id', 'pnode_name',
        'row_is_current', 'total_lmp_da', 'type', 'version_nbr', 'voltage', 'zone',
        'marginal_loss_price_da']
    params = {
        'sort': 'datetime_beginning_ept',
        'startRow': 1,
        'isActiveMetadata': 'true',
        'fields': ','.join(fields),
        'datetime_beginning_ept': datetime.now().strftime(datetime_fmt_query),
        'row_is_current': '1',
        'pnode_id': '33092371',
        'subscription-key': os.environ['PJM_API_KEY'],
        'format': 'json',
        'download': 'true'
    }

    formatted_params = '&'.join([f'{key}={params[key]}' for key in params])
    url = f'https://api.pjm.com/api/v1/da_hrl_lmps?{formatted_params}'

    response = requests.get(url)

    trigger_threshold = Decimal(os.environ['TRIGGER_THRESHOLD'])
    trigger_points = []
    current_trigger_point = {'begin': None, 'end': None}

    output_rows = []

    for row in response.json():
        row['datetime_beginning_cpt'] = from_tz.localize(
            datetime.strptime(row['datetime_beginning_ept'], datetime_fmt_response)).astimezone(
                to_tz)
        if Decimal(row['total_lmp_da']) >= Decimal(30):
            if current_trigger_point['begin'] is None:
                current_trigger_point['begin'] = row['datetime_beginning_cpt']
        elif current_trigger_point['begin'] is not None:
            current_trigger_point['end'] = row['datetime_beginning_cpt'] - timedelta(minutes=1)
            trigger_points.append(current_trigger_point)
            current_trigger_point = {'begin': None, 'end': None}

        output_rows.append(
            'Start local time: {} ComEd: {} Congestion: {} Losses: {} should trigger: {}'.format(
                row['datetime_beginning_cpt'].strftime(datetime_fmt_display),
                locale.currency(Decimal(row['total_lmp_da']), grouping=True),
                locale.currency(Decimal(row['congestion_price_da']), grouping=True),
                locale.currency(Decimal(row['marginal_loss_price_da']), grouping=True),
                'yes' if Decimal(row['total_lmp_da']) >= trigger_threshold else 'no'
            )
        )
    
    if current_trigger_point['begin'] is not None:
        trigger_points.append(current_trigger_point)

    output = ''

    try:
        with open('note.txt') as fh:
            note = fh.read()
            if note:
                output = f'{output}\n{note}\n\n-----\n'
    except:
        pass
    
    output = '{}\nTrigger points (assuming threshold of >= {}):'.format(
        output,
        locale.currency(trigger_threshold, grouping=True))
    for point in trigger_points:
        begin = point['begin'].strftime(datetime_fmt_display)
        if point['end'] is None:
            end = 'Data doesn\'t indicate an end to the trigger'
        else:
            end = point['end'].strftime(datetime_fmt_display)
        output = f'{output}\nFrom {begin} to {end}'

    output = f'{output}\n\n-----\n'

    for row in output_rows:
        output = f'{output}\n{row}'
    
    print(output)
    return output

def send_email(output):
    sg = sendgrid.SendGridAPIClient(apikey=os.environ['SENDGRID_API_KEY'])
    from_email = Email(os.environ['FROM_EMAIL'])
    to_email = Email(os.environ['TO_EMAIL'])
    date = datetime.now().strftime('%m/%d/%Y')
    subject = f'Day-Ahead LMP for {date}'
    plain_content = Content('text/plain', output)
    html_content = Content('text/html', output.replace('\n', '<br>'))
    mail = Mail(from_email, subject, to_email)
    mail.add_content(plain_content)
    mail.add_content(html_content)
    response = sg.client.mail.send.post(request_body=mail.get())

if __name__ == '__main__':
    send_email(fetch_data())