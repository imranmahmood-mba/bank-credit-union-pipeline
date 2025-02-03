import streamlit as st
import requests

# FastAPI backend URL
API_URL = "http://localhost:8000/query/"

# Streamlit UI
st.set_page_config(page_title="Bank Data Query Assistant", layout="wide")

st.title("🔍 Bank & Credit Union Data Query")

# Chat input box
user_input = st.text_input("Ask a question about bank data:", "")

if user_input:
    with st.spinner("Processing..."):
        response = requests.post(API_URL, json={"question": user_input})
        
        if response.status_code == 200:
            data = response.json()
            sql_query = data["query"]
            results = data["results"]

            st.subheader("Generated SQL Query")
            st.code(sql_query, language="sql")

            st.subheader("Query Results")
            if results:
                st.write(results)
            else:
                st.warning("No results found.")
        else:
            st.error("Error querying the database. Please try again.")
