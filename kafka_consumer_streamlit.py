import streamlit as st
import pandas as pd
import plotly.express as px
from streamlit_option_menu import option_menu
from numerize.numerize import numerize
import time
# from streamlit_extras.metric_cards import style_metric_cards
# st.set_option('deprecation.showPyplotGlobalUse', False)
import plotly.graph_objs as go
from kafka import KafkaConsumer
import json
import time
from datetime import datetime
#uncomment this line if you use mysql
#from query import *

st.set_page_config(page_title="Dashboard",page_icon="🌍",layout="wide")
# st.header("ANALYTICAL PROCESSING, KPI, TRENDS & PREDICTIONS")

#all graphs we use custom css not streamlit 
theme_plotly = None 

@st.experimental_memo(ttl=10, max_entries=1000,show_spinner=False)# <--- to edit after full integrate
def get_data(mode=1):
    consumer = KafkaConsumer(
        'newer',  # Replace with your Kafka topic <--- edit the name
        bootstrap_servers=['localhost:9092'],  # Replace with your Kafka server
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    messages = []

    # Consume messages for 10 seconds
    end_time = time.time() + 7
    for message in consumer:
        messages.append(message.value)
        if time.time() > end_time:
            break

    # Create DataFrame from consumed messages
    df = pd.DataFrame(messages)
    if mode == 1:
        return df
    elif mode ==2:
        return df
    else:
        return df



now = datetime.now()





df=get_data()
st.table(df)
time.sleep(10)
st.experimental_rerun()






#df=get_data()

#def main():
#    st.title("Streamlit Kafka Consumer")

    #data_stream = get_data()

    #for data in data_stream:
        # Update your Streamlit UI with the received data
#    while True:
#    	st.write("Received data:", pd.read_json(get_data()))

#if __name__ == "__main__":
#    main()

#while True:
#    get_data()
#    st.table(df)
#    time.sleep(6)
