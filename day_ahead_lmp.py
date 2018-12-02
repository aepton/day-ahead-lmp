import base64
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
from xhtml2pdf import pisa

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

    highlight_style = 'style="font-weight:bold"'
    comed_style = 'style="background-color:green"'
    highlight_cell = 'style="background-color:yellow"'
    output_rows = []
    table_html = f"""
        <table>
            <th>
                <tr>
                    <td>Start local time</td>
                    <td {comed_style}>ComEd</td>
                    <td>Congestion</td>
                    <td>Losses</td>
                    <td>Run generators?</td>
                </tr>
            </th>
    """

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

        local_time = row['datetime_beginning_cpt'].strftime(datetime_fmt_display)
        comed_price = locale.currency(Decimal(row['total_lmp_da']), grouping=True)
        congestion_price = locale.currency(Decimal(row['congestion_price_da']), grouping=True)
        loss_price = locale.currency(Decimal(row['marginal_loss_price_da']), grouping=True)
        trigger_yn = 'yes' if Decimal(row['total_lmp_da']) >= trigger_threshold else 'no'

        if trigger_yn == 'yes':
            row_style = highlight_style
            table_row_style = highlight_cell
            comed_override = highlight_cell
        else:
            row_style = ''
            table_row_style = ''
            comed_override = comed_style

        output_rows.append(
            f'<p><div>Start local time: {local_time}</div><div {row_style}>ComEd: {comed_price}</div><div>Congestion: {congestion_price}</div><div>Losses: {loss_price}</div><div>Run generators: {trigger_yn}</div></p>'
        )
        table_html = f"""
            {table_html}
            <tr {table_row_style}>
                <td>{local_time}</td>
                <td {comed_override}>{comed_price}</td>
                <td>{congestion_price}</td>
                <td>{loss_price}</td>
                <td>{trigger_yn}</td>
            </tr>
        """

    if current_trigger_point['begin'] is not None:
        trigger_points.append(current_trigger_point)

    result_file = open(f'/tmp/trigger_{trigger_threshold}.pdf', 'w+b')
    pisa.CreatePDF(table_html, dest=result_file)
    result_file.close()

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
    if len(trigger_points) == 0:
        output = f'{output}\nNone today'
    for point in trigger_points:
        begin = point['begin'].strftime(datetime_fmt_display)
        if point['end'] is None:
            end = 'Data doesn\'t indicate an end to the trigger'
        else:
            end = point['end'].strftime(datetime_fmt_display)
        output = f'{output}\nFrom {begin} to {end}'

    output = f'{output}\n\n-----\n'

    for row in output_rows:
        output = f'{output}{row}'
    
    return output

def send_email(output):
    tt = Decimal(os.environ['TRIGGER_THRESHOLD'])
    with open(f'/tmp/trigger_{tt}.pdf', 'rb') as f:
        data = f.read()
        f.close()
    encoded = base64.b64encode(data).decode()

    attachment = Attachment()
    attachment.content = encoded
    attachment.type = 'application/pdf'
    attachment.filename = f'trigger_{tt}.pdf'
    attachment.disposition = 'attachment'
    attachment.content_id = 'content ID'

    sg = sendgrid.SendGridAPIClient(apikey=os.environ['SENDGRID_API_KEY'])
    from_email = Email(os.environ['FROM_EMAIL'])
    to_email = Email(os.environ['TO_EMAIL'])
    date = datetime.now().strftime('%m/%d/%Y')
    trigger_threshold = locale.currency(tt, grouping=True)
    subject = f'Day-Ahead LMP for {date} ({trigger_threshold} trigger)'
    plain_content = Content('text/plain', output)
    html_content = Content('text/html', output.replace('\n', '<br>'))
    mail = Mail(from_email, subject, to_email)
    mail.add_content(plain_content)
    mail.add_content(html_content)
    mail.add_attachment(attachment)
    sg.client.mail.send.post(request_body=mail.get())

if __name__ == '__main__':
    send_email(fetch_data())