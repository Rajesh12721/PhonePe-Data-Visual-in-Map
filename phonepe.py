import os
import pandas as pd
import mysql.connector
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="12345",
    database = "phonepe",
    auth_plugin='mysql_native_password'
)
mycursor = mydb.cursor()
path = 'data'
root = os.listdir(path)
for dir in root:
    if dir =='aggregated':
        name1 = dir
        sub_path = path+'\\'+dir
        sub_files = os.listdir(sub_path)
        for sub_dir in sub_files:
            if sub_dir == 'transaction':
                name2 = sub_dir
                sub_subpath = sub_path+'\\'+sub_dir
                sub_subfiles = os.listdir(sub_subpath)
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
                                    state_spl = state_list
                                    temp_list = []
                                    for i in state_spl:
                                        if i.isalnum():
                                            temp_list.append(i)
                                    name3=''.join(temp_list)
                                    statename_path = state_path+'\\'+state_list
                                    statename_dir = os.listdir(statename_path)
                                    for year in statename_dir:
                                        name4 = str(year)
                                        year_path = statename_path+'\\'+year
                                        year_dir = os.listdir(year_path)
                                        i = 1
                                        for file in year_dir:
                                            name5 = str(i)
                                            file_path = year_path+'\\'+file
                                            df = pd.read_json(file_path)
                                            t_name = "create table "+name1+name2+name3+name4+name5+"(name varchar(255),count int, amount decimal(25,0))"
                                            mycursor.execute(t_name)
                                            for item in df['data']['transactionData']:
                                                name = item['name']
                                                for payment in item['paymentInstruments']:
                                                    count = payment['count']
                                                    amount = payment['amount']                                             
                                                query = "insert into "+name1+name2+name3+name4+name5+"(name,count,amount) values (%s,%s,%s)"
                                                value = (name,count,amount)
                                                mycursor.execute(query,value)
                                            i += 1
            else:
                name2 = sub_dir
                sub_subpath = sub_path+'\\'+sub_dir
                sub_subfiles = os.listdir(sub_subpath)
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
                                    state_spl = state_list
                                    temp_list = []
                                    for i in state_spl:
                                        if i.isalnum():
                                            temp_list.append(i)
                                    name3=''.join(temp_list)
                                    statename_path = state_path+'\\'+state_list
                                    statename_dir = os.listdir(statename_path)
                                    for year in statename_dir:
                                        name4 = str(year)
                                        year_path = statename_path+'\\'+year
                                        year_dir = os.listdir(year_path)
                                        i = 1
                                        for file in year_dir:
                                            name5 = str(i)
                                            file_path = year_path+'\\'+file
                                            df = pd.read_json(file_path)
                                            try:
                                                t_name = "create table "+name1+name2+name3+name4+name5+"(brand varchar(255),count int,percentage decimal(25,0))"
                                                mycursor.execute(t_name)
                                                for item in df['data']['usersByDevice']:
                                                    brand = item['brand']
                                                    count = item['count']
                                                    percent = item['percentage']
                                                    query = "insert into "+name1+name2+name3+name4+name5+"(brand,count,percentage) values (%s,%s,%s)"
                                                    value = (brand,count,percent)
                                                    mycursor.execute(query,value)
                                                    i += 1
                                            except:
                                                pass
    elif dir == 'map':
        name1 = dir
        sub_path = path+'\\'+dir
        sub_files = os.listdir(sub_path)
        for sub_dir in sub_files:
            if sub_dir == 'transaction':
                name2 = sub_dir
                sub_subpath = sub_path+'\\'+sub_dir
                sub_subfiles = os.listdir(sub_subpath)
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
                                        state_spl = state_list
                                        temp_list = []
                                        for i in state_spl:
                                            if i.isalnum():
                                                temp_list.append(i)
                                        name3=''.join(temp_list)
                                        statename_path = state_path+'\\'+state_list
                                        statename_dir = os.listdir(statename_path)
                                        for year in statename_dir:
                                            name4 = str(year)
                                            year_path = statename_path+'\\'+year
                                            year_dir = os.listdir(year_path)
                                            i = 1
                                            for file in year_dir:
                                                name5 = str(i)
                                                file_path = year_path+'\\'+file
                                                df = pd.read_json(file_path)
                                                t_name = "create table "+name1+name2+name3+name4+name5+"(name varchar(255),count int, amount decimal(25,0))"
                                                mycursor.execute(t_name)
                                                for item in df['data']['hoverDataList']:
                                                    name = item['name']
                                                    for payment in item['metric']:
                                                        count = payment['count']
                                                        amount = payment['amount']                                             
                                                    query = "insert into "+name1+name2+name3+name4+name5+"(name,count,amount) values (%s,%s,%s)"
                                                    value = (name,count,amount)
                                                    mycursor.execute(query,value)
                                                i += 1
            else:
                name2 = sub_dir
                sub_subpath = sub_path+'\\'+sub_dir
                sub_subfiles = os.listdir(sub_subpath)
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
                                        state_spl = state_list
                                        temp_list = []
                                        for i in state_spl:
                                            if i.isalnum():
                                                temp_list.append(i)
                                        name3=''.join(temp_list)
                                        statename_path = state_path+'\\'+state_list
                                        statename_dir = os.listdir(statename_path)
                                        for year in statename_dir:
                                            name4 = str(year)
                                            year_path = statename_path+'\\'+year
                                            year_dir = os.listdir(year_path)
                                            i = 1
                                            for file in year_dir:
                                                name5 = str(i)
                                                file_path = year_path+'\\'+file
                                                df = pd.read_json(file_path)
                                                t_name = "create table "+name1+name2+name3+name4+name5+"(name varchar(255),registeruser int)"
                                                mycursor.execute(t_name)
                                                for item in df['data']['hoverData']:
                                                    name = item
                                                    registered = df['data']['hoverData'][name]['registeredUsers']
                                                    query = "insert into "+name1+name2+name3+name4+name5+"(name,registeruser) values (%s,%s)"
                                                    value = (name,registered)
                                                    mycursor.execute(query,value)
                                                i += 1
    elif dir == 'top':
        name1 = dir
        sub_path = path+'\\'+dir
        sub_files = os.listdir(sub_path)
        for sub_dir in sub_files:
            if sub_dir == 'transaction':
                name2 = sub_dir
                sub_subpath = sub_path+'\\'+sub_dir
                sub_subfiles = os.listdir(sub_subpath)
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
                                    state_spl = state_list
                                    temp_list = []
                                    for i in state_spl:
                                        if i.isalnum():
                                            temp_list.append(i)
                                    name3=''.join(temp_list)
                                    statename_path = state_path+'\\'+state_list
                                    statename_dir = os.listdir(statename_path)
                                    for year in statename_dir:
                                        name4 = str(year)
                                        year_path = statename_path+'\\'+year
                                        year_dir = os.listdir(year_path)
                                        i = 1
                                        for file in year_dir:
                                            name5 = str(i)
                                            file_path = year_path+'\\'+file
                                            df = pd.read_json(file_path)
                                            dt_name = "create table "+name1+name2+name3+name4+name5+"districts(entityname varchar(255),count int, amount decimal(25,0))"
                                            mycursor.execute(dt_name)
                                            for item in df['data']['districts']:
                                                e_name = item['entityName']
                                                count = item['metric']['count']
                                                amount = item['metric']['amount']
                                                query = "insert into "+name1+name2+name3+name4+name5+"districts(entityname,count,amount) values (%s,%s,%s)"
                                                value = (e_name,count,amount)
                                                mycursor.execute(query,value)
                                            pt_name = "create table "+name1+name2+name3+name4+name5+"pincodes(entityname varchar(255),count int, amount decimal(25,0))"
                                            mycursor.execute(pt_name)
                                            for item in df['data']['pincodes']:
                                                e_name = item['entityName']
                                                count = item['metric']['count']
                                                amount = item['metric']['amount']
                                                query = "insert into "+name1+name2+name3+name4+name5+"pincodes(entityname,count,amount) values (%s,%s,%s)"
                                                value = (e_name,count,amount)
                                                mycursor.execute(query,value)
                                            i += 1
            else:
                name2 = sub_dir
                sub_subpath = sub_path+'\\'+sub_dir
                sub_subfiles = os.listdir(sub_subpath)
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
                                    state_spl = state_list
                                    temp_list = []
                                    for i in state_spl:
                                        if i.isalnum():
                                            temp_list.append(i)
                                    name3=''.join(temp_list)
                                    statename_path = state_path+'\\'+state_list
                                    statename_dir = os.listdir(statename_path)
                                    for year in statename_dir:
                                        name4 = str(year)
                                        year_path = statename_path+'\\'+year
                                        year_dir = os.listdir(year_path)
                                        i = 1
                                        for file in year_dir:
                                            name5 = str(i)
                                            file_path = year_path+'\\'+file
                                            df = pd.read_json(file_path)
                                            dt_name = "create table "+name1+name2+name3+name4+name5+"districts(name varchar(255),registereduser int)"
                                            mycursor.execute(dt_name)
                                            for item in df['data']['districts']:
                                                name = item['name']
                                                reguser = item['registeredUsers']
                                                query = "insert into "+name1+name2+name3+name4+name5+"districts(name,registereduser) values (%s,%s)"
                                                value = (name,reguser)
                                                mycursor.execute(query,value)
                                            pt_name = "create table "+name1+name2+name3+name4+name5+"pincodes(name varchar(255),registereduser int)"
                                            mycursor.execute(pt_name)
                                            for item in df['data']['pincodes']:
                                                name = item['name']
                                                reguser = item['registeredUsers']
                                                query = "insert into "+name1+name2+name3+name4+name5+"pincodes(name,registereduser) values (%s,%s)"
                                                value = (name,reguser)
                                                mycursor.execute(query,value)
                                            i += 1
