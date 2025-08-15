# Libraries
import pandas as pd
import numpy as np

import streamlit as st

import gspread
from google.oauth2.service_account import Credentials
import os

# Excel file open
drive_credentials = Credentials.from_service_account_file("googleDrive_secrets.json", scopes= ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
drive_daily_log_filename="dailySandboxLog_Urh"

print("\nAuthenticating Google Drive API ...")
try:
    googleDrive_client = gspread.authorize(drive_credentials)
    print("~> Authentication to Google Drive API successful! :)")
except Exception as e:
    print("~> Error authenticating to Google Drive API: {} :(".format(e))

print("\nOpening and preparing Daily Log file {} ...".format(drive_daily_log_filename))
try:
    daily_log_file = googleDrive_client.open(drive_daily_log_filename)
    daily_log_sheet = daily_log_file.get_worksheet(0)
    daily_log_data = daily_log_sheet.get_all_values()
    daily_log_df = pd.DataFrame(daily_log_data[1:], columns=daily_log_data[0]) 
    print("~> Daily Log file succesfully imported and available for formating! :)")
except Exception as e:
    print("Error opening Daily Log: {}".format(e))


# Format data

daily_log_df["ymd"] = daily_log_df["Year"] + "-" + daily_log_df["Month"] + "-" + daily_log_df["Day"]

# https://docs.streamlit.io/
# https://streamlit.io/gallery


st.title("Daily statistics dashboard")
st.write("This is the data from daily excel file:")

# st.table(daily_log_df)
st.dataframe(daily_log_df)
# st.dataframe(daily_log_df.style.highlight_max(axis=0))

# st.line_chart(daily_log_df[["ymd", "Resting HR"]])

# x = st.slider('x') 
# st.write(x, 'squared is', x * x)

# st.text_input("Your name", key="name")
# st.session_state.name

# if st.checkbox('Show dataframe'):
#     chart_data = pd.DataFrame(np.random.randn(20, 3), columns=['a', 'b', 'c'])
#     st.line_chart(chart_data)

# df = pd.DataFrame({'first column': [1, 2, 3, 4], 'second column': [10, 20, 30, 40]})
# option = st.selectbox('Which number do you like best?', df['first column'])
# 'You selected: ', option

os.system('streamlit run src/streamlit_sample.py --server.port 8501')
# streamlit run src/streamlit_sample.py --server.port 8501