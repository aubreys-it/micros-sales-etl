import logging
import csv
import json
import pandas as pd
import azure.functions as func
from azure.storage.blob import ContainerClient
from datetime import date

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    xlsURI = req.params.get("uri")
    csvSAS = '?' + req.params.get("sas").replace('_', '&')
    csvURI = "https://domesdaydiag.blob.core.windows.net/product-mix"
    return_dict = {}

    if not xlsURI:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('uri')

    if not csvSAS:
        csvSAS = req_body.get('sas')

    xls_file = xlsURI[xlsURI.find('MISALES/') + 8:]

    if xls_file.upper().endswith('.XLS'):

        csv_file = xls_file[:-4] + '.csv'
        csv_rows = []
        row = []

        with open(xlsURI) as xls_object:
            csv_lines = csv.DictReader(xls_object, delimiter=',')

            for line in csv_lines:
                if line['Unnamed: 0'].find('Period From') >= 0:
                    rpt_date = line['Unnamed: 0'][-10:]

                if not line['Unnamed: 0'].isdigit():
                    if line['Unnamed: 0'].find(' - ') >= 0:
                        loc_id = xls_file[11:13]

                    elif line['Unnamed: 5'].find('Disc') >= 0:
                        row = ['0555555', 'Discount', 1, line['Unnamed: 8'], rpt_date, loc_id, date.today()]

                else:
                    if line['Unnamed: 0'] is not None:
                        item_id = line['Unnamed: 0'].zfill(7)
                        row = [item_id, line['Unnamed: 1'], line['Unnamed: 4'], line['Unnamed: 8'], rpt_date, loc_id, date.today()]

                if row:
                    csv_rows.append(row)

        if row:
            df = pd.DataFrame(csv_rows)

            blob_container = ContainerClient.from_container_url(csvURI + csvSAS)
            blob_client = blob_container.get_blob_client(csv_file)
            blob_client.upload_blob(data=df.to_csv(index=False, header=False))

            return_dict['func_return'] = 'True'

    else:
        return_dict['func_return'] = False

    return func.HttpResponse(
        json.dumps(
            return_dict
        ),
        mimetype='application/json'
    )
