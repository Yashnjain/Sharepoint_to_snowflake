# -*- coding: utf-8 -*-
"""

"""

import sys
import logging
from datetime import datetime
import pandas as pd
import numpy as np
import os
import sharepy
import yaml
import json
import re
# import dropbox
import email.message
import smtplib
import time
from bu_snowflake import get_connection
import bu_alerts

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)


global variable_dict 
# global log_file_location

log_file_location = os.getcwd() + '\\' + 'logs' + '\\' + 'RISK_SHP_SF_LOGS.txt'
if os.path.isfile(log_file_location):
    os.remove(log_file_location)

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] - %(message)s',
    filename=log_file_location)

receiver_email = 'indiapowerit@biourja.com, DAPower@biourja.com'

# receiver_email = 'rahul.chaturvedi@biourja.com'
def spdownload(spt, pathname, filename, sheetname):
    """Downloads a file from Sharepoint.

    Args:
        spt : the connected Sharepoint account
        pathname : the path to the file
        filename : the file needed to be retrieved
        sheetname : the particuar sheet in question

    Returns:
        df : the dataframe containing contents of the sheet
        or
        None : wherein the file does not exist
    """
    print('Downloading {}'.format(filename))

    target = local_dir + os.path.basename(filename)

    try:
        r = spt.getfile(site + path.format(pathname,
                                           filename), filename=target)
        df = pd.read_excel(target, sheet_name=sheetname, parse_dates=True)
        os.remove(target)
        return (r, df)

    except Exception as ex:
        print("Exception caught during execution: ", ex)
        logging.exception(f'Exception caught during execution: {ex}')
        raise ex

def get_table_columns(databasename,schemaname, tablename):
    """
    Retrieves a list of the column headers of the table.

    Args:
        schemename : the schema of the database
        tablename : the table in question

    Returns:
        cols_in_db : the list of the column headers of the table
        or
        [] : if error arises when retrieving columns
    """
    sql = '''
    select *
    from {}
    limit 1
    '''.format(schemaname+'.'+tablename)
    try:
        conn = get_connection(role=f'OWNER_{databasename}',
                              database=databasename, schema=schemaname)
        conn.cursor().execute('USE WAREHOUSE BUIT_WH')

        cs = conn.cursor()
        cs.execute(sql)

        df = pd.DataFrame.from_records(
            iter(cs), columns=[x[0] for x in cs.description])
        conn.close()
        cols_in_db = [re.sub(r'\W', '', x.upper()) for x in df.columns]

        return cols_in_db
    except Exception as e:
        print(f"Exception caught during execution: ", e)
        logging.exception(f'Exception caught during execution: {e}')
        raise e





def get_temp_df(pathname, filename, sheetname):
    """Retrieves file from Sharepoint.

    Args:
        path name : the path to the file
        filename : the file in question
        sheetname : the particular sheet of the file

    Returns:
        df : the dataframe with the contents of the excel file
        or
        None : wherein the file does not exist
    """
    dfs = []
    try:
        for sheet in sheetname:
            variable_dict = {}
            (r, df) = spdownload(s, pathname, filename, sheet)
            if r.status_code != 200:
                variable_dict['reason'] = 'File does not exist on Sharepoint'
                j = '[' + json.dumps(variable_dict) + ']'
                return None
            dfs.append(df)
        final = pd.concat(dfs)
        if sheet == 'BNP Unrealized' and filename == 'Derivative Position Summary.xlsm':
            final = final.iloc[:, :-1]  #removing last column from BNP Unrealized sheet df
        return final
    except Exception as e:
        print(f"Exception caught during execution: ", e)
        logging.exception(f'Exception caught during execution: {e}')
        raise e


def inv_df_maker(df):
    try:
        
        colList = list(df.columns)
        for col in range(len(colList)):
            if len(list(df.loc[df[colList[col]]=='Site'].index)):
                    contactCol = col
                    contractIndex = df.loc[df[colList[col]]=='Site'].index[-1]
        for i in range(0,contractIndex):
            df.drop(i,inplace=True)
        for i in range(0,contactCol):
            df.drop(df.columns[[i]], axis=1, inplace=True)
        df.reset_index(inplace=True,drop=True)
        df.drop(0,inplace=True)
        df.columns=[
                        "SITE",
                        "WAREHOUSE_CODE",
                        "WAREHOUSE_NAME",
                        "LOCATION",
                        "BRANCH",
                        "PRODUCT_ACCESS_CODE",
                        "MATERIAL_TYPE",
                        "GLOBAL_GRADE",
                        "GRADE",
                        "SIZE",
                        "OD_IN",
                        "OD_IN_2",
                        "WALL_IN",
                        "HEAT_CONDITION",
                        "LOT_SERIAL_NUMBER",
                        "HEAT_NUMBER",
                        "DATE_LAST_RECEIPT",
                        "AGE",
                        "MATERIAL_OWNER",
                        "STOCK_OWNER",
                        "MILL",
                        "MILL_NAME",
                        "COUNTRY_OF_ORIGIN",
                        "PRODUCTION_HOLD",
                        "HOLD_REASON",
                        "HOLD_REMARK",
                        "MATERIAL_CLASSIFICATION",
                        "LENGTH_CODE",
                        "ONHAND_PIECES",
                        "ONHAND_LENGTH_FT",
                        "ONHAND_LENGTH_IN",
                        "ONHAND_PIECE_LENGTH",
                        "ONHAND_WEIGHT_LBS",
                        "ONHAND_VALUE_DOLLARS",
                        "ONHAND_DOLLARS_PER_POUNDS",
                        "RESERVED_PIECES",
                        "RESERVED_LENGTH_FT",
                        "RESERVED_LENGTH_IN",
                        "RESERVED_WEIGHT_LBS",
                        "RESERVED_VALUE_DOLLARS",
                        "AVAILABLE_PIECES",
                        "AVAILABLE_LENGTH_FT",
                        "AVAILABLE_LENGTH_IN",
                        "AVAILABLE_WEIGHT_LBS",
                        "AVAILABLE_VALUE_DOLLARS"]                    
        return df    
    except Exception as e:
        logging.exception(f'Error {e}')
        raise e



def upload_df_driver_to_db(df_driver):
    global JOB_ID 
    global filename
    global databasename
    global schemaname
    global tablename 
    global column_preserve 
    global min_rows 
    global to_addr

    for i, x in df_driver.iterrows():
        variable_dict = {}
        
        try:
            # Assign a random job id for linking job status purpose for the same file.
            # For example, a job "Started", you want to have one "Completed", or "Abandoned",
            # or "Failed" with the same JOB_ID.
            
            JOB_ID = np.random.randint(1000000, 9999999)
            # Get job related information
            pathname = x['XLSXFILELOCATION'] + "/"
            pathname = re.sub("'", "''", pathname)
            filename = x['XLSXFILENAME']
            sheetname = x['SHEETNAME'].split(";")
            jobname = x['DEPARTMENT']
            
            # databasename = 'POWERDB'
            databasename = x['DATABASENAME']
            schemaname = databasename + '.' + str(x['SCHEMA']).upper().strip()
            tablename = re.sub(r'\W', '', str(x['TABLENAME']).upper().strip())
            column_preserve = str(x['COLUMN_PRESERVE']).upper().strip()
            min_rows = x['ROW_CHECK_MINIMUM']
            # to_addr = [addr for addr in str(x['EMAIL_LIST']).split(';') if '@' in addr]
            to_addr = str(x['EMAIL_LIST']).replace(';', ',')
            # to_addr = 'radha.waswani@biourja.com'
            variable_dict = dict(((k, eval(k)) for k in ('JOB_ID','filename', 'databasename',
                                                         'schemaname', 'tablename',
                                                         'column_preserve', 'min_rows', 'to_addr')))
            print("To_addr ************", to_addr)
            # Change to json string
            
            j = '[' + json.dumps(variable_dict) + ']'
            # log_file_location = os.getcwd() + '\\' + 'logs' + '\\' + '{}_ALERT.txt'.format(tablename)
            
            # log_file_location = bu_alerts.add_file_logging(logger,process_name='{}_ALERT'.format(tablename))
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s [%(levelname)s] - %(message)s',
                filename=log_file_location)
            bu_alerts.bulog(process_name=f'{jobname}_SHAREPOINT_SNOWFLAKE', database='POWERDB', status='Started',
                            table_name=schemaname + '.' + tablename, row_count=0, log=str(j), warehouse="ITPYTHON_WH", process_owner='IMAM')

            # Retrieve data from excel file

            df = get_temp_df(pathname, filename, sheetname)
            if filename == "Daily On Hand Inventory Report v2.1.xlsx":
                df = inv_df_maker(df)
            if df is None:
                # File could not be retrieved, move onto next row in table
                continue
            primary_key_list = []
            table = schemaname + '.' + tablename
            query_primary_key = f'''SHOW PRIMARY KEYS IN {table}'''
            conn = get_connection(role=f'OWNER_{databasename}',
                                  database=databasename, schema=schemaname)
            cursor = conn.cursor()
            cursor.execute(query_primary_key)
            result = cursor.fetchall()
            if len(result) > 0:
                for j in range(0, len(result)):
                    primary_key_list.append(result[j][4].upper())
                print("Primary keys for table are ", primary_key_list)
                df.columns = [re.sub(r'\W', '', x.upper()) for x in df.columns]
                print(df)
                df.rename({'TOTALQUANTITYLOTSSIZE': 'TOTAL_QUANTITY', 'TOTALQUANTITY': 'TOTAL_QUANTITY'}, axis=1, inplace=True)
                
                df.drop_duplicates(subset=primary_key_list,
                                   keep='first', inplace=True)
                print(df)
            # Change column headers to all caps and remove whitespace
            # df.columns = [re.sub(r'\W', '', x.upper()) for x in df.columns]

            # If a column contains only null values, make its data type str
            # for col in df.columns:
            #    if df[col].isnull().sum() == len(df):
            #        df[col] = df[col].astype(str)

            # Abandon job if data frame has fewer rows than min_rows, log abandoned job and reason

            if len(df) < min_rows:
                print('{} abandoned. FILE ROWS SMALLER THAN min_rows.'.format(filename))
                variable_dict['file_rows'] = len(df)
                variable_dict['reason'] = 'FILE ROWS SMALLER THAN min_rows'
                j = '[' + json.dumps(variable_dict) + ']'
                bu_alerts.bulog(process_name=f'{jobname}_SHAREPOINT_SNOWFLAKE', database='POWERDB', status='Failed',
                                table_name=schemaname + '.' + tablename, row_count=0, log=str(j), warehouse="ITPYTHON_WH", process_owner='IMAM')
                if len(to_addr) > 0:
                    logging.info('FILE ROWS SMALLER THAN min_rows.')
                    bu_alerts.send_mail(
                        receiver_email=to_addr,
                        mail_subject=f'JOB FAILED {jobname}_SHP->SF - {0}'.format(
                            schemaname+"."+tablename),
                        mail_body='{0} failed during execution, Attached logs'.format(
                            schemaname+"."+tablename),
                        attachment_location=log_file_location
                    )
                    
                # Invalid data frame, move onto next row in table
                continue

            # Create DDL
            all_cols = list(df.columns)

            # Check if table already exists and compare columns with excel file
            cols_in_file = all_cols
            cols_in_db = get_table_columns(databasename,schemaname, tablename)

            # Add insert/update columns to match those in the Snowflake table
            add_insert = False
            add_update = False
            orig_cols_in_db = len(cols_in_db)
            df["INSERT_DATE"] = datetime.now()
            df["UPDATE_DATE"] = datetime.now()

            if "INSERT_DATE" not in cols_in_db and orig_cols_in_db != 0:
                add_insert = True
                cols_in_db.append("INSERT_DATE")
            if "UPDATE_DATE" not in cols_in_db and orig_cols_in_db != 0:
                add_update = True
                cols_in_db.append("UPDATE_DATE")

            if orig_cols_in_db != 0 and cols_in_db.index("UPDATE_DATE") < cols_in_db.index("INSERT_DATE"):
                cols_in_file.append("UPDATE_DATE")
                cols_in_file.append("INSERT_DATE")
            else:
                cols_in_file.append("INSERT_DATE")
                cols_in_file.append("UPDATE_DATE")

            df = df[cols_in_file]
            print(cols_in_db, cols_in_file, cols_in_db == cols_in_file)

            # Group columns based on datatype: date/time, int, float
            dt_cols = df.select_dtypes(
                include=['datetime', 'datetime64']).columns.tolist()
            int_cols = df.select_dtypes(
                include=['int', 'int64']).columns.tolist()
            float_cols = df.select_dtypes(
                include=['float', 'float64']).columns.tolist()

            # Make table of column names with their respective datatype
            df_cols = pd.DataFrame(list(zip(all_cols, ['string']*len(all_cols))),
                                   columns=['COLUMN', 'DATATYPE'])
            df_cols.loc[df_cols['COLUMN'].isin(
                dt_cols), 'DATATYPE'] = 'datetime'
            df_cols.loc[df_cols['COLUMN'].isin(
                int_cols), 'DATATYPE'] = 'number'
            df_cols.loc[df_cols['COLUMN'].isin(
                float_cols), 'DATATYPE'] = 'number(38,6)'

            # If the column name needn't be preserved, add trailing underscore
            if column_preserve == 'N':
                df_cols['COLUMN'] = df_cols['COLUMN'] + '_ '
                all_cols = list(df.columns)

            df_cols['ITEM'] = df_cols['COLUMN'] + ' ' + df_cols['DATATYPE']
            # sql = 'create table if not exists {} ({})'.format(tablename,
            #                                                   ",".join(df_cols['ITEM'].values))

            # if orig_cols_in_db > 0: #and cols_in_db != cols_in_file
            #     print('{} ABANDONED. FILE COLUMNS DO NOT MATCH TABLE.'.format(filename))
            #     variable_dict['reason'] = 'TABLE EXISTS BUT COLUMNS DO NOT MATCH'
            #     variable_dict['suggestions'] = 'DROP TABLE AND RETRY'
            #     j = '['+json.dumps(variable_dict)+']'
            #     logging.info(
            #         '{} ABANDONED. FILE COLUMNS DO NOT MATCH TABLE.'.format(filename))
            #     bu_alerts.bulog(process_name='RISK_SHAREPOINT_SNOWFLAKE', database='POWERDB', status='Failed',
            #                     table_name=schemaname + '.' + tablename, row_count=0, log=str(j), warehouse="ITPYTHON_WH", process_owner='IMAM')
            #     if len(to_addr) > 0:
            #         bu_alerts.send_mail(
            #             receiver_email=to_addr,
            #             mail_subject='JOB FAILED RISK-SHP->SF - {0}'.format(
            #                 schemaname+"."+tablename),
            #             mail_body='{0} failed during execution, Attached logs'.format(
            #                 schemaname+"."+tablename),
            #             attachment_location=log_file_location
            #         )
            #        # Invalid columns, move onto next row in table
            #     continue

            # Everything looks good to proceed, save as csv file
            csv_file = r'c:/temp/risk_sharepoint_snowflake.csv'
            #Droping trade date from bnp_volume
            if 'MACQUAIRE_VOLUME' in tablename:
                final_table_columns = ['Commodity Code', 'Exchange Instrument Code', 'Future/Option', 'Delivery Month', 'Product Name', 'Total Quantity (Gallons)', 'INSERT_DATE', 'UPDATE_DATE']
                df = df[df.columns.intersection(final_table_columns)]
                df = df[final_table_columns]
                df['Delivery Month']= df['Delivery Month'].dt.date
                # df.drop(columns = ['Trade Date'], inplace=True)
                # df['Exercise Price'] = df['Exercise Price'].replace('                     ', np.NaN)
                # df['Option P&L'] = df['Option P&L'].replace('                     ', np.NaN)
                # df['Last day of trading (expiry date)'] = pd.to_datetime(df['Last day of trading (expiry date)'],  utc=False)
                # df['Last day of trading (expiry date)'] = df['Last day of trading (expiry date)'].dt.date
                # df['Input Date'] = pd.to_datetime(df['Input Date'],  utc=False)
                # df['Input Date'] = df['Input Date'].dt.date
                # df['Con Input Date'] = pd.to_datetime(df['Con Input Date'],  utc=False)
                # df['Con Input Date'] = df['Con Input Date'].dt.date
            df.to_csv(csv_file, index=False, date_format='%Y-%m-%d %H:%M:%S')

            # Run DDL statement, create table if not exists
            
            

            conn = get_connection(role=f'OWNER_{databasename}',
                                  database=databasename, schema=schemaname)
            conn.cursor().execute('USE WAREHOUSE BUIT_WH')
            conn.cursor().execute('USE DATABASE {}'.format(databasename))
            conn.cursor().execute('USE SCHEMA {}'.format(schemaname))
            # conn.cursor().execute(sql)

            # Truncate table, remove staging file if any, upload file to staging,
            # and copy into table
            conn.cursor().execute("truncate table {}".format(tablename))
            conn.cursor().execute("remove @%{}".format(tablename))
            if add_insert:
                conn.cursor().execute("alter table {} add column INSERT_DATE datetime".format(tablename))
                print("insert date column added")
            if add_update:
                conn.cursor().execute("alter table {} add column UPDATE_DATE datetime".format(tablename))
                print("update date column added")
            conn.cursor().execute("PUT file://{} @%{} overwrite=true".format(csv_file, tablename))
            conn.cursor().execute('''
                    COPY INTO {} file_format=(type=csv
                    skip_header=1 field_optionally_enclosed_by = '"' empty_field_as_null=true escape_unenclosed_field=None)
                    '''.format(tablename))
            if filename == "Daily On Hand Inventory Report v2.1.xlsx":
                duplicate_query=f'''delete from {tablename} where SITE is NULL;'''
                cur = conn.cursor()
                cur.execute(duplicate_query)

            conn.close()
            logging.warning(
                '{} ...{} rows uploaded.'.format(tablename, len(df)))

            # Remove csv file from local drive
            os.remove(csv_file)

            # Bulog complete job
            j = '[' + json.dumps(variable_dict) + ']'
            bu_alerts.bulog(process_name=f'{jobname}_SHAREPOINT_SNOWFLAKE', database='POWERDB', status='Completed',
                            table_name=schemaname + '.' + tablename, row_count=len(df), log=str(j), warehouse="ITPYTHON_WH", process_owner='IMAM')
            if len(to_addr) > 0:
                bu_alerts.send_mail(
                    receiver_email=to_addr,
                    mail_subject='JOB SUCCESS {0}_SHP->SF - {1}'.format(jobname,
                        schemaname+"."+tablename),
                    mail_body='{0} completed successfully, Attached logs'.format(
                        schemaname+"."+tablename),
                    attachment_location=log_file_location
                )
        except Exception as e:
            try:
                os.remove(csv_file)
            except:
                print("no file to remove")
            print('Exception caught during execution: ', e)
            logging.exception(f'Exception caught during execution: {e}')

            # Bulog failed job
            logging.error('{} failed.'.format(filename))
            variable_dict['Error'] = "Maybe SQL Error"
            j = '[' + json.dumps(variable_dict) + ']'
            bu_alerts.bulog(process_name=f'{jobname}_SHAREPOINT_SNOWFLAKE', database='POWERDB', status='Failed',
                            table_name=schemaname + '.' + tablename, row_count=0, log=str(j), warehouse="ITPYTHON_WH", process_owner='IMAM')

            if len(to_addr) > 0:
                bu_alerts.send_mail(
                    receiver_email=to_addr,
                    mail_subject='JOB FAILED - {0}_SHP->SF - {1}'.format(jobname,
                        schemaname+"."+tablename),
                    mail_body='{0} failed during execution, Attached logs'.format(
                        schemaname+"."+tablename),
                    attachment_location=log_file_location
                )
                # sendmail(to_addr, 'Sharepoint -> Snowflake: {0}, {1} failed to upload to {2}.'.format(filename, sheetname, schemaname+'.'+tablename),
                #         str(e))

            continue
        


# %%
if __name__ == "__main__":
    logging.info('Execution Started')
    rows = 0
    starttime = datetime.now()
    logging.warning('Start work at {} ...'.format(
        starttime.strftime('%Y-%m-%d %H:%M:%S')))
    try:
        j = '[{"CURRENT_DATETIME": "'+str(datetime.now())+'"}]'
        bu_alerts.bulog(process_name='RISK_SHAREPOINT_SNOWFLAKE_DRIVER', database='POWERDB', status='Started',
                        table_name='', row_count=0, log=str(j), warehouse="ITPYTHON_WH", process_owner='IMAM')
        # Authentication of Sharepoint (with YAML) and Dropbox (with TOKEN) accounts
        here = os.path.dirname(os.path.abspath(__file__))
        yaml_file = os.path.join(here, 'credentials.yaml')

        f = yaml.safe_load(open(yaml_file, 'r'))
        username = f['username']
        password = f['password']
        site = 'https://biourja.sharepoint.com'
        path1 = "/itdev/_api/web/GetFolderByServerRelativeUrl"
        path2 = "('Shared Documents{0}')/Files('{1}')/$value"
        path = path1 + path2
        
        # Connecting to Sharepoint and downloading the file with sync params
        s = sharepy.connect(site, username, password)
        spfile = r'SNOWFLAKE_SYNC_PARAMS.xlsx'
        local_dir = r'C:/temp/'

        (r, df_driver) = spdownload(s, "/IT", spfile, 'PARAMS')
        df_driver.columns = [x.upper() for x in df_driver.columns]
        df_driver['ROW_CHECK_MINIMUM'] = df_driver['ROW_CHECK_MINIMUM'].fillna(
            1).astype(int)
        df_driver = df_driver[df_driver['SYNC_NOW'] == 'Y']
        # df_driver = df_driver.iloc[1:2]

        j = df_driver.to_json(orient='records')

        upload_df_driver_to_db(df_driver)
        bu_alerts.bulog(process_name='RISK_SHAREPOINT_SNOWFLAKE_DRIVER', database='POWERDB', status='Completed',
                        table_name='', row_count=len(df_driver), log=str(j), warehouse="ITPYTHON_WH", process_owner='IMAM')
    except Exception as e:
        print("Exception caught during execution: ", e)
        logging.exception(f'Exception caught during execution: {e}')
        bu_alerts.bulog(process_name='RISK_SHAREPOINT_SNOWFLAKE_DRIVER', database='POWERDB', status='Failed',
                        table_name='', row_count=len(df_driver), log=str(j), warehouse="ITPYTHON_WH", process_owner='IMAM')
        bu_alerts.send_mail(
            receiver_email=receiver_email,
            mail_subject='JOB FAILED - RISK_SHAREPOINT_SNOWFLAKE_DRIVER',
            mail_body='RISK_SHAREPOINT_SNOWFLAKE_DRIVER failed due to {}'.format(e)
        )

    endtime = datetime.now()
    print('Complete work at {} ...'.format(
        endtime.strftime('%Y-%m-%d %H:%M:%S')))
    print('Total time taken: {} seconds'.format(
        (endtime-starttime).total_seconds()))

# %%
