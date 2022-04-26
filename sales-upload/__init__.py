######################################################################################################
#   micros-sales-etl
#   2022-04-26
#   Grant Houser
#
#   Takes MISales .XLS report files generated by MICROS and uploaded to BLOB storage,
#   converts to .CSV and uploads to another BLOB container for BULK INSERT into DMCP
#
######################################################################################################

import logging
import json
import pandas as pd
import azure.functions as func
from azure.storage.blob import ContainerClient
from datetime import date

# SAS signatures come in with certain special characters replaced to keep from throwing errors
def fixSAS(sas):
    sas = sas.replace(':', '%3A')
    sas = sas.replace('+', '%2B')
    return sas

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # xlsURI + SAS are for file origination point
    # csvURI + SAS are for file destination
    xlsURI = req.params.get("uri")
    xlsSAS = '?' + req.params.get('xlsSAS').replace('_', '&')
    csvSAS = '?' + req.params.get("csvSAS").replace('_', '&')
    csvURI = "https://domesdaydiag.blob.core.windows.net/product-mix"
    
    return_dict = {}    #JSON return values

    # Destination file subfolders based on location IDs
    loc_dict = {
            '02': 'POWELL',
            '03': 'SUNSPOT',
            '04': 'CEDARBLUFF',
            '05': 'MARYVILLE',
            '06': 'HIXSON',
            '08': 'LENOIRCITY',
            '09': 'PAPERMILL',
            '10': 'BISTRO',
            '11': 'CLEVELAND',
            '12': 'BLUETICK',
            '13': 'OAKRIDGE',
            '14': 'STRAWPLAINS',
            '15': 'FIELDHOUSE',
            '16': 'GREENEVILLE',
            '17': 'BRISTOL',
            '18': 'MORRISTOWN',
            '21': 'JOHNSONCITY',
            '22': 'HARDINVALLEY',
            '23': 'SEVIERVILLE'
    }

    # Get header info
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

        # Reform Shared Access Signatures
        xlsSAS = fixSAS(xlsSAS)
        csvSAS = fixSAS(csvSAS)

        # Extract container, file name, and path from URI
        xls_container_name = xlsURI[:xlsURI.find('.net') + 5 + xlsURI[xlsURI.find('.net') + 5:].find('/')]
        xls_file = xlsURI[xlsURI.find('MISALES/') + 8:]
        xls_path = xlsURI[len(xls_container_name) + 1:xlsURI.find(xls_file)]

        if xls_file.upper().endswith('.XLS'):

            csv_file = xls_file[:-4] + '.csv'   # Destination File Name
            csv_rows = []                       # List of lists to hold csv values
            row = []                            # List to hold one csv line a t a time

            # Connect to BLOB storage
            xls_container = ContainerClient.from_container_url(xls_container_name + xlsSAS)
            xls_client = xls_container.get_blob_client(xls_path + xls_file)
            xls_stream = xls_client.download_blob().content_as_bytes()

            # Store .XLS file as Dataframe
            data_xls = pd.read_excel(xls_stream)

            # Get location ID from file name
            loc_id = xls_file[11:13]

            # Get destination subfolder name based on loc_id
            loc_name = loc_dict.get(loc_id)
            if not loc_name:
                loc_name = 'UNKNOWN'

            # Return full BLOB destination path + file name
            return_dict['new_blob_path'] = xls_container_name + '/' + loc_name + '/MISALES/' + xls_file

            # Convert dataframe to string only data type
            for col in data_xls.columns:
                data_xls[col] = data_xls[col].astype('string')

            # Extract CSV data from Dataframe
            for line in data_xls.iterrows():
                if not line[1].isnull()[0]:
                    if line[1]['Unnamed: 0'].find('Period From') >= 0:
                        rpt_date = line[1]['Unnamed: 0'][-10:]      # Report date (also the date sold for all items)

                    elif line[1]['Unnamed: 0'].isdigit():   # Sales item data lines start with item id number
                        if line[1] is not None:
                            item_id = line[1]['Unnamed: 0'].zfill(7) # Item IDs should be 7 digits long
                            # Build csv row
                            row = [item_id, line[1]['Unnamed: 1'], line[1]['Unnamed: 4'], line[1]['Unnamed: 8'], rpt_date, loc_id, date.today()]

                elif not line[1].isnull()[5]:
                    if line[1]['Unnamed: 5'].find('Disc') >= 0:     # Look for Discount information
                        row = ['0555555', 'Discount', 1, line[1]['Unnamed: 8'], rpt_date, loc_id, date.today()]
                if row:
                    csv_rows.append(row)
                    row = []

            if csv_rows:
                data_csv = pd.DataFrame(csv_rows)

                # Create new BLOB with CSV data
                csv_container = ContainerClient.from_container_url(csvURI + csvSAS)
                csv_client = csv_container.get_blob_client(csv_file)
                csv_client.upload_blob(data=data_csv.to_csv(index=False, header=False, line_terminator='\r\n'))

                # Remove original BLOB
                xls_container.delete_blob(xls_path + xls_file)

                return_dict['func_return'] = True   # Function successful
                
            else:
                return_dict['func_return'] = False  # Function not successful
    #'''
    else:
        return_dict['func_return'] = False  # Function not successful
    
    return func.HttpResponse(
        json.dumps(
            return_dict
        ),
        mimetype='application/json'
    )
