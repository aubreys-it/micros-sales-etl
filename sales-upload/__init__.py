import logging
import json
import pandas as pd
import azure.functions as func
from azure.storage.blob import ContainerClient
from datetime import date

def fixSAS(sas):
    sas = sas.replace(':', '%3A')
    sas = sas.replace('+', '%2B')
    
    return sas

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    xlsURI = req.params.get("uri")
    xlsSAS = '?' + req.params.get('xlsSAS').replace('_', '&')
    csvSAS = '?' + req.params.get("csvSAS").replace('_', '&')
    csvURI = "https://domesdaydiag.blob.core.windows.net/product-mix"
    return_dict = {}

    if not xlsURI:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            uri = req_body.get('uri')

    if not csvSAS:
        csvSAS = '?' + req_body.get('csvSAS').replace('_', '&')

    if not xlsSAS:
        xlsSAS = '?' + req_body.get('xlsSAS').replace('_', '&')

    if xlsURI and csvSAS and xlsSAS:

        xlsSAS = fixSAS(xlsSAS)
        csvSAS = fixSAS(csvSAS)

        xls_container = xlsURI[:xlsURI.find('.net') + 5 + xlsURI[xlsURI.find('.net') + 5:].find('/')]
        xls_file = xlsURI[xlsURI.find('MISALES/') + 8:]
        xls_path = xlsURI[len(xls_container) + 1:xlsURI.find(xls_file)]

        return_dict['xlsURI'] = xlsURI
        return_dict['csvSAS'] = csvSAS
        return_dict['xlsSAS'] = xlsSAS
        return_dict['xls_container'] = xls_container
        return_dict['xls_file'] = xls_file
        return_dict['xls_path'] = xls_path
        '''
        if xls_file.upper().endswith('.XLS'):

            csv_file = xls_file[:-4] + '.csv'
            csv_rows = []
            row = []

            xls_container = ContainerClient.from_container_url(xls_container + xlsSAS)
            xls_client = xls_container.get_blob_client(xls_path + xls_file)
            xls_stream = xls_client.download_blob().content_as_bytes()
            data_xls = pd.read_excel(xls_stream)

            loc_id = xls_file[11:13]

            for col in data_xls.columns:
                data_xls[col] = data_xls[col].astype('string')

            for line in data_xls.iterrows():
                if not line[1].isnull()[0]:
                    if line[1]['Unnamed: 0'].find('Period From') >= 0:
                        rpt_date = line[1]['Unnamed: 0'][-10:]

                    elif line[1]['Unnamed: 0'].isdigit():
                        if line[1] is not None:
                            item_id = line[1]['Unnamed: 0'].zfill(7)
                            row = [item_id, line[1]['Unnamed: 1'], line[1]['Unnamed: 4'], line[1]['Unnamed: 8'], rpt_date, loc_id, date.today()]

                elif not line[1].isnull()[5]:
                    if line[1]['Unnamed: 5'].find('Disc') >= 0:
                        row = ['0555555', 'Discount', 1, line[1]['Unnamed: 8'], rpt_date, loc_id, date.today()]
                if row:
                    csv_rows.append(row)
                    row = []

            if csv_rows:
                data_csv = pd.DataFrame(csv_rows)

                csv_container = ContainerClient.from_container_url(csvURI + csvSAS)
                csv_client = csv_container.get_blob_client(csv_file)
                csv_client.upload_blob(data=data_csv.to_csv(index=False, header=False))

                xls_container.delete_blob(xls_path + xls_file)

                return_dict['func_return'] = True
                
            else:
                return_dict['func_return'] = False
    '''
    else:
        return_dict['func_return'] = False
    
    return func.HttpResponse(
        json.dumps(
            return_dict
        ),
        mimetype='application/json'
    )
