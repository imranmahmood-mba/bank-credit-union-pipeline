# Ensure you are using the correct imports
from langchain.chat_models import ChatOpenAI
from langchain_experimental.sql import SQLDatabaseChain
from langchain.sql_database import SQLDatabase
from google.cloud import bigquery
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
import os
import json

# Initialize FastAPI
app = FastAPI()

client = bigquery.Client()


# Initialize OpenAI LLM with explicit instructions
llm = ChatOpenAI(model_name="gpt-4", temperature=0.0)  # Set temperature to 0 for deterministic responses

# Connect to BigQuery
db = SQLDatabase.from_uri("bigquery://alpha-rank-ai/financial_institutions")

# Create SQL generation chain with STRICT instructions
query_chain = SQLDatabaseChain.from_llm(
    llm,
    db,
    verbose=True,
    return_direct=False
)


def format_results(bigquery_results):
    """Format BigQuery query output into a structured dictionary."""
    if not bigquery_results:
        return []

    column_names = [field.name for field in bigquery_results.schema]
    return [dict(zip(column_names, row)) for row in bigquery_results]


class QueryRequest(BaseModel):
    question: str

# Add both route patterns so they both point to the same function.
@app.post("/query", include_in_schema=False)
@app.post("/query/")
def generate_sql(request: QueryRequest):
    try:
        print(f"Received Request: {request}")
        # Generate and execute SQL query using LangChain (your logic)
        raw_results = query_chain.run(request.question)
        return {"query": request.question, "results": raw_results}
    except Exception as e:
        print(f"Error executing query: {e}")
        raise HTTPException(status_code=500, detail=str(e))
