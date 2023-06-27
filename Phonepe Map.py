import streamlit as slt
import mysql.connector
import pandas as pd
from urllib.request import urlopen
import json
import csv
import plotly.express as px
#MySQL Database Connection
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="12345",
    database = "phonepe", #Database Name
    auth_plugin='mysql_native_password'
)
mycursor = mydb.cursor()
#Streamlit Design
#Title
slt.title("Phonepe Data state wise")
type = ['aggregated','map','top']
select = slt.sidebar.selectbox("Select the type :", type)
type_input = ['transaction','user']
select_input = slt.sidebar.selectbox("Select the Input :",type_input)
year = ['2018','2019','2020','2021','2022']
select_Year = slt.sidebar.selectbox("Select the Channel Id:",year)
quater = ['1','2','3','4']
select_quater = slt.sidebar.selectbox("Select the Quater :", quater)
out = select+select_input
#CSV File Open
fp = open("csvfile/fetch.csv","w+")
fp.truncate()
#Transaction data Updaton
if select_input == 'transaction':
    query = "select * from "+out+" where year="+select_Year+" and quater="+select_quater+";"
    mycursor.execute(query)
    colnames = [desc[0] for desc in mycursor.description]
    data = mycursor.fetchall()
    myFile = csv.writer(fp)
    myFile.writerow(colnames)
    myFile.writerows(data)
#User data Updaton
else:
    query = "select * from "+out+" where year="+select_Year+" and quater="+select_quater+";"
    mycursor.execute(query)
    colnames = [desc[0] for desc in mycursor.description]
    data = mycursor.fetchall()
    myFile = csv.writer(fp)
    myFile.writerow(colnames)
    myFile.writerows(data)
fp.close()
#Map Building
with urlopen('https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson') as response:
    states = json.load(response)
#Read CSV File
df = pd.read_csv("csvfile/fetch.csv",dtype={"year": str})
#State name Change in DataFrame
state_cor = {'andaman-&-nicobar-islands':'Andaman & Nicobar', 'andhra-pradesh' : 'Andhra Pradesh', 'arunachal-pradesh':'Arunachal Pradesh', 'assam':'Assam', 'bihar':'Bihar', 'chandigarh':'Chandigarn', 'chhattisgarh':'Chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu', 'delhi':'Delhi', 'goa':'Goa', 'gujarat':'Gujarat', 'haryana':'Haryana', 'himachal-pradesh':'Himachal Pradesh', 'jammu-&-kashmir':'Jammu & Kashmir', 'jharkhand':'Jharkhand', 'karnataka':'Karnataka', 'kerala':'Kerala', 'ladakh':'Ladakh', 'lakshadweep':'Lakshadweep', 'madhya-pradesh':'Madhya Pradesh', 'maharashtra':'Maharashtra', 'manipur':'Manipur', 'meghalaya':'Meghalaya', 'mizoram':'Mizoram', 'nagaland':'Nagaland', 'odisha':'Odisha', 'puducherry':'Puducherry', 'punjab':'Punjab', 'rajasthan':'Rajasthan', 'sikkim':'Sikkim', 'tamil-nadu':'Tamil Nadu', 'telangana':'Telangana', 'tripura':'Tripura', 'uttar-pradesh':'Uttar Pradesh', 'uttarakhand':'Uttarakhand', 'west-bengal':'West Bengal'}
state_update = []
for state_name in df['state']:
    temp = state_cor[state_name]
    state_update.append(temp)
df['state'] = state_update
slt.write(df)
#Map Visible in Data Wise
if out == 'aggregatedtransaction':
    fig = px.choropleth(df, locations='state', geojson=states, 
                        featureidkey="properties.ST_NM", 
                        color='amount', hover_name="state",
                        hover_data="count",
                        projection="mercator",
                        color_continuous_scale=px.colors.sequential.Plasma, 
                        range_color =(1000000,600000000), 
                        scope="asia",
                        labels={'amount':'Total Amount'}
                        )
elif out == 'aggregateduser':
    fig = px.choropleth(df, locations='state', geojson=states, 
                        featureidkey="properties.ST_NM", 
                        color='count', hover_name="state",
                        projection="mercator",
                        color_continuous_scale=px.colors.sequential.Plasma, 
                        range_color =(100000,1500000),
                        scope="asia",
                        labels={'count':'User Mobile Count'}
                        )
elif out == 'maptransaction':
    fig = px.choropleth(df, locations='state', geojson=states, 
                        featureidkey="properties.ST_NM", 
                        color='amount', hover_name="state",
                        hover_data="count",
                        projection="mercator",
                        color_continuous_scale=px.colors.sequential.Plasma, 
                        range_color =(1000000,5000000000), 
                        scope="asia",
                        labels={'amount':'Total Amount'}
                        )
elif out == 'mapuser':
    fig = px.choropleth(df, locations='state', geojson=states, 
                        featureidkey="properties.ST_NM", 
                        color='registeruser', hover_name="state",
                        projection="mercator",
                        color_continuous_scale=px.colors.sequential.Plasma, 
                        range_color =(10000,5000000),
                        scope="asia",                                                    
                        labels={'registeruser':'Register Users'}
                        )
elif out == 'toptransaction':
    fig = px.choropleth(df, locations='state', geojson=states, 
                        featureidkey="properties.ST_NM", 
                        color='amount', hover_name="state",
                        hover_data="count",
                        projection="mercator",
                        color_continuous_scale=px.colors.sequential.Plasma, 
                        range_color =(1000000,4000000000), 
                        scope="asia",
                        labels={'amount':'Total Amount'}
                        )
else:
    fig = px.choropleth(df, locations='state', geojson=states, 
                        featureidkey="properties.ST_NM", 
                        color='registeruser', hover_name="state",
                        projection="mercator",
                        color_continuous_scale=px.colors.sequential.Plasma, 
                        range_color =(100,1000000),
                        scope="asia",                                                    
                        labels={'registeruser':'Register Users'}
                        )
fig.update_geos(fitbounds="locations", visible=False)
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()