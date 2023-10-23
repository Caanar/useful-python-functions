# This app requires connection to a specified folder where the SQL Queries are stored.

from fastapi import FastAPI
import uvicorn  
import nest_asyncio 
import pandas as pd
import psycopg2
from sshtunnel import SSHTunnelForwarder
from datetime import date
from fastapi.responses import FileResponse
from os import listdir
from os.path import isfile, join
from openpyxl import load_workbook
from openpyxl.workbook.protection import WorkbookProtection

#%% 

nest_asyncio.apply()
app = FastAPI(title='Custom Reports', description='This app will allow you to get the following Custom Reports: reports hidden for confidentiality purposes')

Account = '1'
Report = 'Report Name'
Email = 'camilo.arrieta@workemail.com' # Only valid emails will be able to access reports

@app.get('/custom_report') 
def custom_report(Account: str, Report: str, Email: str):
    tunnel = SSHTunnelForwarder(('sqlproxy.imsdev.io', 22), ssh_private_key='id_rsa', ssh_username='carrieta', ssh_private_key_password='super_secret', remote_bind_address=('read.add.net', 5432))
    tunnel.start()
    conn = psycopg2.connect(database='db',user='carrieta',host='localhost',password='secret',port= tunnel.local_bind_port,options="""-c search_path="public" """)
    cur = conn.cursor()
    cur.execute('''select a."name", a.id from accounts a order by a."name"''')
    rows = cur.fetchall()
    accounts = pd.DataFrame(rows)
    acc_name = accounts[accounts[1] == int(Account)][0].iloc[0]
    def func():
        try:
            cur.execute(open(r"C:/Users/CArrieta/Desktop/gui/queries/{}".format(Report)).read().replace(':account_id', str(Account)))
            colnames = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            df = pd.DataFrame(rows)
            i = 0
            for col in colnames:
                df.rename(columns={i: col},inplace=True)
                i+=1
            try:
                cur.execute('''select us.email, us.firstname, us.lastname from users us where us.account_id = {}'''.format(Account))
                rows = cur.fetchall()
                admin_emails = pd.DataFrame(rows)
                admin_emails.set_index(0, inplace=True)
                if Email in admin_emails.index:                    
                    df.to_excel('C:/Users/CArrieta/Desktop/gui/excel/{0} {1} Report {2}.xlsx'.format(acc_name, Report, date.today()), sheet_name='{0}'.format(Report))
                    writer = pd.ExcelWriter('C:/Users/CArrieta/Desktop/gui/excel/{0} {1} Report {2}.xlsx'.format(acc_name, Report, date.today()), engine='xlsxwriter')
                    df.to_excel(writer, sheet_name='{0}'.format(Report), index=False)
                    worksheet = writer.sheets['{0}'.format(Report)]  # pull worksheet object
                    for idx, col in enumerate(df):  # loop through all columns
                        series = df[col]
                        max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 1  
                        worksheet.set_column(idx, idx, max_len)
                    writer.close()
                    
                    print('Success!!! That Email Exists in the selected account and belongs to {0} {1}'.format(admin_emails.loc[['{}'.format(Email)]][1][0],admin_emails.loc[['{}'.format(Email)]][2][0]))
                    return FileResponse(path="C:/Users/CArrieta/Desktop/gui/excel/{0} Report {1}.xlsx".format(Account,date.today()),
                                        filename="{0} Report {1}.xlsx".format(Account,date.today()))            
                else:
                    return 'Beware: that email is not used by any Profile on that account'
            except:
                return 'Something went wrong. Please verify if the Account ID is correct. If it doesnt work submit a Customer Support Ticket'
        except:
            return 'That is not a valid Report, please write one of the Reports from the beginning'
    return(func())

    conn.close()
 
if __name__ == '__main__': 
    uvicorn.run(app)

#onlyfiles = [f for f in listdir('C:/Users/CArrieta/Desktop/gui/queries') if isfile(join('C:/Users/CArrieta/Desktop/gui/queries', f))]
#for e in onlyfiles:
#    custom_report(1, e, 'camilo.arrieta@workemail.com')

#custom_report(1, 'Report Name', 'camilo.arrieta@workemail.com')
