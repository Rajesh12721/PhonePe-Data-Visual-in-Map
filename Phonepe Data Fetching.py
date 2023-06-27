import os
import pandas as pd
import mysql.connector
#MySQL Database Connection
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="12345",
    database = "phonepe", #Database Name
    auth_plugin='mysql_native_password'
)
mycursor = mydb.cursor()
path = 'data'
root = os.listdir(path)
for dir in root:
    #Aggregated Data Path
    if dir =='aggregated':
        name1 = dir
        sub_path = path+'\\'+dir
        sub_files = os.listdir(sub_path)
        for sub_dir in sub_files:
            #Transaction Data
            if sub_dir == 'transaction':
                name2 = sub_dir
                sub_subpath = sub_path+'\\'+sub_dir
                sub_subfiles = os.listdir(sub_subpath)
                #Table Creation
                t_name = "create table "+name1+name2+"(state varchar(255), year int, quater int, payment_type varchar(255), count int, amount decimal(25,0));"
                mycursor.execute(t_name)
                for country in  sub_subfiles:
                    country_path = sub_subpath+'\\'+country
                    country_dir = os.listdir(country_path)
                    for india in country_dir:
                        india_path = country_path+'\\'+india
                        india_dir = os.listdir(india_path)
                        for state in india_dir:
                            if state == 'state':
                                state_path = india_path+'\\'+state
                                state_dir = os.listdir(state_path)
                                for state_list in state_dir:
                                    state_name = state_list
                                    statename_path = state_path+'\\'+state_list
                                    statename_dir = os.listdir(statename_path)
                                    for year in statename_dir:
                                        year_no = year
                                        year_path = statename_path+'\\'+year
                                        year_dir = os.listdir(year_path)
                                        i = 1
                                        for file in year_dir:
                                            quater_no = i
                                            file_path = year_path+'\\'+file
                                            df = pd.read_json(file_path)
                                            for item in df['data']['transactionData']:
                                                payment_name = item['name']
                                                for payment in item['paymentInstruments']:
                                                    count = payment['count']
                                                    amount = payment['amount']   
                                                #Insert Value  
                                                query = "insert into "+name1+name2+"(state,year,quater,payment_type,count,amount) values (%s,%s,%s,%s,%s,%s);"
                                                value = (state_name,year_no,quater_no,payment_name,count,amount)
                                                mycursor.execute(query,value)
                                            i += 1
                                            mydb.commit()
            #User Data
            else:
                name2 = sub_dir
                sub_subpath = sub_path+'\\'+sub_dir
                sub_subfiles = os.listdir(sub_subpath)
                #Table Creation
                t_name = "create table "+name1+name2+"(state varchar(255),year int,quater int,brand varchar(255),count int,percentage decimal(25,0));"
                mycursor.execute(t_name)
                for country in  sub_subfiles:
                    country_path = sub_subpath+'\\'+country
                    country_dir = os.listdir(country_path)
                    for india in country_dir:
                        india_path = country_path+'\\'+india
                        india_dir = os.listdir(india_path)
                        for state in india_dir:
                            if state == 'state':
                                state_path = india_path+'\\'+state
                                state_dir = os.listdir(state_path)
                                for state_list in state_dir:
                                    state_name = state_list
                                    statename_path = state_path+'\\'+state_list
                                    statename_dir = os.listdir(statename_path)
                                    for year in statename_dir:
                                        year_no = year
                                        year_path = statename_path+'\\'+year
                                        year_dir = os.listdir(year_path)
                                        i = 1
                                        for file in year_dir:
                                            quater_no = i
                                            file_path = year_path+'\\'+file
                                            df = pd.read_json(file_path)
                                            try:
                                                for item in df['data']['usersByDevice']:
                                                    brand = item['brand']
                                                    count = item['count']
                                                    percent = item['percentage']
                                                    #Insert values
                                                    query = "insert into "+name1+name2+"(state,year,quater,brand,count,percentage) values (%s,%s,%s,%s,%s,%s);"
                                                    value = (state_name, year_no, quater_no, brand,count,percent)
                                                    mycursor.execute(query,value)
                                            except:
                                                pass
                                            i += 1
                                            mydb.commit()
    #Map Data Path
    elif dir == 'map':
        name1 = dir
        sub_path = path+'\\'+dir
        sub_files = os.listdir(sub_path)
        for sub_dir in sub_files:
            #Transaction Data
            if sub_dir == 'transaction':
                name2 = sub_dir
                sub_subpath = sub_path+'\\'+sub_dir
                sub_subfiles = os.listdir(sub_subpath)
                #Table Creation
                t_name = "create table "+name1+name2+"(state varchar(255),year int,quater int, name varchar(255),count int, amount decimal(25,0));"
                mycursor.execute(t_name)
                for hover in  sub_subfiles:
                    hover_path = sub_subpath+'\\'+hover
                    hover_dir = os.listdir(hover_path)
                    for country in  hover_dir:
                        country_path = hover_path+'\\'+country
                        country_dir = os.listdir(country_path)
                        for india in country_dir:
                            india_path = country_path+'\\'+india
                            india_dir = os.listdir(india_path)
                            for state in india_dir:
                                if state == 'state':
                                    state_path = india_path+'\\'+state
                                    state_dir = os.listdir(state_path)
                                    for state_list in state_dir:
                                        state_name = state_list
                                        statename_path = state_path+'\\'+state_list
                                        statename_dir = os.listdir(statename_path)
                                        for year in statename_dir:
                                            year_no = year
                                            year_path = statename_path+'\\'+year
                                            year_dir = os.listdir(year_path)
                                            i = 1
                                            for file in year_dir:
                                                quater_no = i
                                                file_path = year_path+'\\'+file
                                                df = pd.read_json(file_path)
                                                for item in df['data']['hoverDataList']:
                                                    name = item['name']
                                                    for payment in item['metric']:
                                                        count = payment['count']
                                                        amount = payment['amount']
                                                    #Insert value                                             
                                                    query = "insert into "+name1+name2+"(state,year,quater,name,count,amount) values (%s,%s,%s,%s,%s,%s);"
                                                    value = (state_name,year_no,quater_no,name,count,amount)
                                                    mycursor.execute(query,value)
                                                i += 1
                                                mydb.commit()
            #User Data
            else:
                name2 = sub_dir
                sub_subpath = sub_path+'\\'+sub_dir
                sub_subfiles = os.listdir(sub_subpath) 
                #Table Creation 
                t_name = "create table "+name1+name2+"(state varchar(255),year int, quater int, name varchar(255),registeruser int);"
                mycursor.execute(t_name)
                for hover in  sub_subfiles:
                    hover_path = sub_subpath+'\\'+hover
                    hover_dir = os.listdir(hover_path)
                    for country in  hover_dir:
                        country_path = hover_path+'\\'+country
                        country_dir = os.listdir(country_path)
                        for india in country_dir:
                            india_path = country_path+'\\'+india
                            india_dir = os.listdir(india_path)
                            for state in india_dir:
                                if state == 'state':
                                    state_path = india_path+'\\'+state
                                    state_dir = os.listdir(state_path)
                                    for state_list in state_dir:
                                        state_name = state_list
                                        statename_path = state_path+'\\'+state_list
                                        statename_dir = os.listdir(statename_path)
                                        for year in statename_dir:
                                            year_no = year
                                            year_path = statename_path+'\\'+year
                                            year_dir = os.listdir(year_path)
                                            i = 1
                                            for file in year_dir:
                                                quater_no = i
                                                file_path = year_path+'\\'+file
                                                df = pd.read_json(file_path)
                                                for item in df['data']['hoverData']:
                                                    name = item
                                                    registered = df['data']['hoverData'][name]['registeredUsers']
                                                    #Insert value
                                                    query = "insert into "+name1+name2+"(state,year,quater,name,registeruser) values (%s,%s,%s,%s,%s);"
                                                    value = (state_name, year_no, quater_no, name, registered)
                                                    mycursor.execute(query,value)
                                                i += 1
                                                mydb.commit()
    #Top Data Path
    if dir=='top':
        name1 = dir
        sub_path = path+'\\'+dir
        sub_files = os.listdir(sub_path)
        for sub_dir in sub_files:
            #Transaction Data
            if sub_dir == 'transaction':
                name2 = sub_dir
                sub_subpath = sub_path+'\\'+sub_dir
                sub_subfiles = os.listdir(sub_subpath)
                #Table Creation
                t_name = "create table "+name1+name2+"(state varchar(255),year int, quater int, type varchar(25),entityname varchar(255),count int, amount decimal(25,0));"
                mycursor.execute(t_name)
                for country in  sub_subfiles:
                    country_path = sub_subpath+'\\'+country
                    country_dir = os.listdir(country_path)
                    for india in country_dir:
                        india_path = country_path+'\\'+india
                        india_dir = os.listdir(india_path)
                        for state in india_dir:
                            if state == 'state':
                                state_path = india_path+'\\'+state
                                state_dir = os.listdir(state_path)
                                for state_list in state_dir:
                                    state_name = state_list
                                    statename_path = state_path+'\\'+state_list
                                    statename_dir = os.listdir(statename_path)
                                    for year in statename_dir:
                                        year_no = year
                                        year_path = statename_path+'\\'+year
                                        year_dir = os.listdir(year_path)
                                        i = 1
                                        for file in year_dir:
                                            quater_no = i
                                            file_path = year_path+'\\'+file
                                            df = pd.read_json(file_path)
                                            for item in df['data']['districts']:
                                                type = 'districts'
                                                e_name = item['entityName']
                                                count = item['metric']['count']
                                                amount = item['metric']['amount']
                                                #Insert Value
                                                query = "insert into "+name1+name2+"(state,year,quater,type,entityname,count,amount) values (%s,%s,%s,%s,%s,%s,%s);"
                                                value = (state_name,year_no,quater_no,type,e_name,count,amount)
                                                mycursor.execute(query,value)
                                            for item in df['data']['pincodes']:
                                                type = 'pincodes'
                                                e_name = item['entityName']
                                                count = item['metric']['count']
                                                amount = item['metric']['amount']
                                                #Insert Value
                                                query = "insert into "+name1+name2+"(state,year,quater,type,entityname,count,amount) values (%s,%s,%s,%s,%s,%s,%s);"
                                                value = (state_name,year_no,quater_no,type,e_name,count,amount)
                                                mycursor.execute(query,value)
                                            i += 1
                                            mydb.commit()
            #User Data
            elif sub_dir == 'user':                                
                name2 = sub_dir
                sub_subpath = sub_path+'\\'+sub_dir
                sub_subfiles = os.listdir(sub_subpath)
                #Table Creation
                t_name = "create table "+name1+name2+"(state varchar(255), year int,quater int,type varchar(25),name varchar(255),registeruser int);"
                mycursor.execute(t_name)
                for country in  sub_subfiles:
                    country_path = sub_subpath+'\\'+country
                    country_dir = os.listdir(country_path)
                    for india in country_dir:
                        india_path = country_path+'\\'+india
                        india_dir = os.listdir(india_path)
                        for state in india_dir:
                            if state == 'state':
                                state_path = india_path+'\\'+state
                                state_dir = os.listdir(state_path)
                                for state_list in state_dir:
                                    state_name = state_list
                                    statename_path = state_path+'\\'+state_list
                                    statename_dir = os.listdir(statename_path)
                                    for year in statename_dir:
                                        year_no = year
                                        year_path = statename_path+'\\'+year
                                        year_dir = os.listdir(year_path)
                                        i = 1
                                        for file in year_dir:
                                            quater_no = i
                                            file_path = year_path+'\\'+file
                                            df = pd.read_json(file_path)
                                            for item in df['data']['districts']:
                                                type = 'districts'
                                                name = item['name']
                                                reguser = item['registeredUsers']
                                                #Insert Value
                                                query = "insert into "+name1+name2+"(state,year,quater,type,name,registeruser) values (%s,%s,%s,%s,%s,%s);"
                                                value = (state_name, year_no,quater_no, type,name,reguser)
                                                mycursor.execute(query,value)
                                            for item in df['data']['pincodes']:
                                                type = 'pincodes'
                                                name = item['name']
                                                reguser = item['registeredUsers']
                                                #Insert Value
                                                query = "insert into "+name1+name2+"(state,year,quater,type,name,registeruser) values (%s,%s,%s,%s,%s,%s);"
                                                value = (state_name,year_no,quater_no,type,name,reguser)
                                                mycursor.execute(query,value)
                                            i += 1
                                            mydb.commit()
            else:
                pass           
print("Completed")
