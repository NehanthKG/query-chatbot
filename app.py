from flask import Flask, request, render_template, jsonify
import requests
import pyodbc
import json
from datetime import datetime, timedelta
import re

# Flask app
app = Flask(__name__)

# Ollama server details
OLLAMA_SERVER = "http://127.0.0.1:11434"
MODEL_NAME = "qwen2.5-coder:latest"

# Database connection details
DB_CONFIG = {
    "server": r"LAPTOP-F0K2Q8PJ\SQLEXPRESS",
    "database": "SalesDB",
    "driver": "{ODBC Driver 17 for SQL Server}",
    "trusted_connection": "yes",
}

def fetch_schema():
    """Fetch schema from the database."""
    connection_str = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
    )
    conn = pyodbc.connect(connection_str)
    cursor = conn.cursor()

    schema_query = """
    SELECT 
        TABLE_NAME, COLUMN_NAME
    FROM 
        INFORMATION_SCHEMA.COLUMNS
    WHERE 
        TABLE_SCHEMA = 'dbo'
    """
    cursor.execute(schema_query)
    schema = {}
    for table, column in cursor.fetchall():
        schema.setdefault(table, []).append(column)

    conn.close()
    return schema

def generate_sql_query(prompt, schema):
    """Generate SQL query using Ollama."""
    schema_text = "\n".join(
        [f"{table}: {', '.join(columns)}" for table, columns in schema.items()]
    )

    addons = "Give me only the sql server 2016 query, I dont want extra words."
    full_prompt = f"Database schema:\n{schema_text}\n\nPrompt:\n{prompt}\n\n{addons}"

    data = {"model": MODEL_NAME, "prompt": full_prompt}
    response = requests.post(f"{OLLAMA_SERVER}/api/generate", json=data)
    sql_query_parts = []

    try:
        for line in response.text.splitlines():
            parsed_line = json.loads(line)
            sql_query_parts.append(parsed_line['response'])

        sql_query = "".join(sql_query_parts)
        return clean_sql_query(sql_query)
    except Exception as e:
        raise Exception(f"Error generating SQL query: {str(e)}")

def clean_sql_query(sql_query):
    """Clean generated SQL query."""
    sql_query = sql_query.replace("  ", " ").strip()
    sql_query = sql_query.replace("sql", "").strip()
    sql_query = re.sub(r'(-)\s*(\d+)', r'\1\2', sql_query)
    sql_query = re.sub(r'\[\s*([A-Za-z0-9_]+)\s*\]', r'\1', sql_query)
    sql_query = sql_query.replace("`", "")
    sql_query = re.sub(r'\s+', ' ', sql_query).strip()
    return sql_query

def execute_query(query):
    """Execute the SQL query."""
    connection_str = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
    )
    conn = pyodbc.connect(connection_str)
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        conn.close()
        return {"columns": columns, "rows": results}
    except Exception as e:
        conn.close()
        raise Exception(f"Query execution failed: {str(e)}")

@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        prompt = request.form['prompt']
        try:
            schema = fetch_schema()
            sql_query = generate_sql_query(prompt, schema)
            print("sql_query) ", sql_query)
            result = execute_query(sql_query)
            return render_template(
                'results.html', 
                prompt=prompt, 
                sql_query=sql_query, 
                columns=result['columns'], 
                rows=result['rows']
            )
        except Exception as e:
            return render_template('error.html', error=str(e))
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
