import streamlit as st 
import pandas as pd 
from sqlalchemy import create_engine, text
import urllib
import wikipedia as w
import movieposters as mp
from datetime import datetime
import re

from sqlalchemy import create_engine
from urllib.parse import quote_plus

username = "rashadmin"
password = "1234@Ra$hik"
server = "rashserveroct.database.windows.net"  # Or the server's hostname/IP
database = "rashdatabase"
driver = "ODBC Driver 17 for SQL Server"

connection_string = f"mssql+pyodbc://{username}:{quote_plus(password)}@{server}/{database}?driver={quote_plus(driver)}"
engine = create_engine(connection_string)

# Streamlit app to search movies
st.title('Rate and Review Movies')

#search =st.text_input('Seach Movies Name: ')


movie_title = st.text_input("review the movie")


if movie_title:
    query= text("SELECT * FROM dbo.movies WHERE title = :movie_title")
    
    with engine.connect() as  conn: 
        df= pd.read_sql_query(query,conn,params={"movie_title": movie_title})
        st.write(w.summary(movie_title))
        st.dataframe(df)

        user_id= st.number_input('Tell me your id: ',min_value=1)
        rating = st.slider('How did you like it?  ',1,5)
        review = st.text_area("Review : ") 
        movieID = df.loc[df['title']== movie_title, 'movieId'].values
        genres = df.loc[df['movieId']== movieID, 'genres'].values
        timestamp = datetime.now()
        st.write(movieID,genres,timestamp)
        if(st.button('submit')):
            print(f'You reviewed {movie_title}')


else:
    st.warning("No matching movies found.")

