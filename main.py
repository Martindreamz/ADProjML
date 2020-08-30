from flask import Flask
import pandas as pd
import pyodbc
import numpy as np
import matplotlib as plt


app = Flask(__name__)

@app.route("/reorder")
def reorder():
    import pandas as pd
    import numpy as np
    import math
    import datetime as dt
    import pandas as pd 
    import pyodbc
    from flask import Flask,request

    server = 'team8-sa50.database.windows.net'
    database = 'ADProj'
    username = 'Bianca'
    password = '!Str0ngPsword'   
    driver= '{ODBC Driver 17 for SQL Server}'
    sql_conn=pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    pyodbc.drivers()
    cursor = sql_conn.cursor()

    def convert_dates(x):
        x['dateOfRequest']=pd.to_datetime(x['dateOfRequest']) #converting date column to datetime format
        x['Month']=x['dateOfRequest'].dt.month #creating a new column 'month' from 'date' using dt.month
        x['Year']=x['dateOfRequest'].dt.year #same - for year
        x.pop('dateOfRequest') #delete 'date' column
        return x
    
    req_query = ''' SELECT [Id]
      ,[EmployeeId]
      ,[dateOfRequest]
      ,[dateOfAuthorizing]
      ,[AuthorizerId]
      ,[status]
      ,[comment]
  FROM [ADProj].[dbo].[Requisition_Table]'''
    requisition_data = pd.read_sql(req_query, sql_conn)
    requisition_data.to_csv("latest_requisition.csv")
#     print(requisition_data.shape)
    requisition_data
    
    req_dt_query = ''' SELECT [Id]
      ,[RequisitionId]
      ,[StationeryId]
      ,[reqQty]
      ,[rcvQty]
      ,[status]
  FROM [ADProj].[dbo].[RequisitionDetail_Table]'''
    detail_data = pd.read_sql(req_dt_query, sql_conn)
    detail_data.to_csv("latest_requisition_detail.csv")
    print(detail_data.shape)
    detail_data
    
    df_merged = detail_data.merge(requisition_data, how='inner', left_on='RequisitionId', right_on='Id')
    df_merged = df_merged[['RequisitionId','StationeryId','reqQty','dateOfRequest']]
    print(df_merged.shape)
    df_merged
    
    df_merged = convert_dates(df_merged)
    df_merged
    
    df_merged.pop('RequisitionId')
    df_new = df_merged.groupby(['Year', 'Month', 'StationeryId']).sum().reset_index()
    df_sorted = df_new.sort_values(['StationeryId','Year','Month'],ascending=[1,1,1]).reset_index(drop=True)
    df_sorted.head()

    uniqueItemCount = df_sorted.StationeryId.unique()
    uniqueItemCount
    
    updatesDict = {}

    #split the df by stationeryID
    for i in uniqueItemCount:
        print('For itemId ',i)
        recommendqtySS = 0
        recommendqtyMV = 0
        finalrecommendqty = 0

    #-------------------     BELOW ARE THE PARTS FOR MOVING AVERAGE     ---------------
        dfSimulateMV = df_sorted[df_sorted["StationeryId"]==i]

        #sort the df by year then month
        dfMV = dfSimulateMV.sort_values(['Year','Month'], ascending=[0,0]).reset_index(drop=True)

        if (dt.date.today().month>6):  
            dfMV = dfMV[(dfMV['Year']==dt.date.today().year) & (dfMV['Month']>=dt.date.today().month-6)]
        else:
            dfMV = dfMV[(dfMV['Year']==(dt.date.today().year-1) ) & (dfMV['Month']>=(6+dt.date.today().month))
                          |(dfMV['Year']==(dt.date.today().year) ) & (dfMV['Month']<(dt.date.today().month))]
    #     print(dfMV)
        dfMF = dfMV.reset_index(drop=True)
        print(dfMV)

        if(not dfMV.empty):

            #check the std deviation in the 3 month if there are big changes
            # if no, #run the movingavg
            # if yes, #skip that month and take another previous month
            meanMV = dfMV.iloc[0:6].sort_values('reqQty',ascending=[1]).iloc[0:3].reqQty.mean()
            print('mean',meanMV)
            j=0
            k=0
            sumMV=0


            while k!=3 and j<dfMV.shape[0]:    
    #             print(abs(dfMV.reqQty[i]-meanMV)/meanMV)

                if(abs(dfMV.reqQty[j]-meanMV)/meanMV<0.5):
                    sumMV+=dfMV.reqQty[j]
                    k+=1

                j+=1
            if(k==0):
                k+=1
            print('based on last 3 months MV',sumMV/k)
            recommendqtyMV=round(sumMV/k)
            print('recommend qty MV',recommendqtyMV)   
    #-------------------     ABOVE ARE THE PARTS FOR MOVING AVERAGE     ---------------
    #-------------------     BELOW ARE THE PARTS FOR SEASONAL PREDICTION     ---------------

        #sort the df by month then year
        dfSimulateSS = df_sorted[df_sorted["StationeryId"]==i]
        dfSS = dfSimulateSS.sort_values(['Month','Year'], ascending=[0,0]).reset_index(drop=True)
    #     print(dfSS)


        curmonth = dt.date.today().month

        dfSS = dfSimulateSS[dfSimulateSS['Month']==curmonth].reset_index(drop=True).sort_values('Year',ascending=0).reset_index(drop=True)
        print(dfSS)
        #run the seasonal prediction

        if(not dfSS.empty):

            meanSS = dfSS.iloc[0:6].sort_values('reqQty',ascending=[1]).iloc[0:3].reqQty.mean()

            recommendqtySS=round(meanSS)
            print('recommend qty SS',recommendqtySS)

        finalrecommendqty =max(recommendqtySS,recommendqtyMV)
        print('final recommendation is', finalrecommendqty)

        if(finalrecommendqty!=0):
            updatesDict[int(i)]=int(finalrecommendqty)
        print('\n')

    #-------------------     ABOVE ARE THE PARTS FOR SEASONAL PREDICTION     --------------
    
    for key,value in updatesDict.items():
        query = '''UPDATE dbo.Stationery_Table SET reOrderQty = ? WHERE Id = ?'''
        var = (value,key)
        print(key,value)
        cursor.execute(query,var)
        sql_conn.commit()
        
    cursor.close()
    sql_conn.close()

    return 'done'

@app.route("/seeder")
def seeder():
    from flask import Flask,request
    import pandas as pd 
    import pyodbc
    # from sqlalchemy import create_engine
    import numpy as np
    

    server = 'team8-sa50.database.windows.net'
    database = 'ADProj'
    username = 'Bianca'
    password = '!Str0ngPsword'   
    driver= '{ODBC Driver 17 for SQL Server}' 
    sql_conn=pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    pyodbc.drivers()
    
    data_new = pd.read_csv('requisitiontesting2.csv', parse_dates = ['dateOfRequest','dateOfAuthorizing'], infer_datetime_format = True,index_col=None)
    print(data_new.dateOfRequest)
    data_new=data_new.fillna("") 
    data_new.head()
    
    cursor = sql_conn.cursor()
    for index, row in data_new.iterrows():
        print(row)
        cursor.execute("INSERT INTO Requisition_Table([dateOfRequest],[dateOfAuthorizing],[status],[comment],[EmployeeId],[AuthorizerId]) values(?,?,?,?,?,?)",

                       row['dateOfRequest'],
                       row['dateOfAuthorizing'],
                       row['status'],
                       row['comment'],
                       row['EmployeeId'],
                       row['AuthorizerId'])

    sql_conn.commit()
    
    reqdetail = pd.read_csv('detail_data.csv')
    reqdetail.head()
    
    cursor = sql_conn.cursor()
    for index, row in reqdetail.iterrows():
        print(row)
        cursor.execute("INSERT INTO RequisitionDetail_Table([RequisitionId],[StationeryId],[reqQty],[rcvQty],[status]) values(?,?,?,?,?)",
                                       row['RequisitionId'],row['StationeryId'],row['reqQty'],row['rcvQty'],row['status'])

    sql_conn.commit()
    cursor.close()
    sql_conn.close()
    
    return 'seeded'


@app.route('/multi/',methods=['GET','POST'])
def multi():
    name = request.args.get('name') or request.form.get('name')
    return "Hello "+ str(name or '')
    

if __name__ == '__main__':
    app.run(debug=True)

