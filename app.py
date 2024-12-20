from flask import Flask, render_template, request, send_file, flash, redirect, url_for
import pandas as pd
import mysql.connector
import re
from datetime import datetime
import os
from dotenv import load_dotenv
import tempfile

load_dotenv()

app = Flask(__name__)
app.secret_key = os.urandom(24)

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv('DB_HOST', 'ls-aa0257c7e0352fe79ede13c8821bbf8515ad8a2b.cbiuqwux1ssg.us-west-2.rds.amazonaws.com'),
        user=os.getenv('DB_USER', 'dbmasteruser'),
        password=os.getenv('DB_PASSWORD', 'Mol=Gg+h?9LK<DNP=>:e&*7b#1y([o`a'),
        database=os.getenv('DB_NAME', 'Tripalong')
    )

def extract_id_from_url(url):
    if pd.isna(url):
        return None
    match = re.search(r'hd-\d+-([\d]+)-', str(url))
    return match.group(1) if match else None

def process_excel_file(file):
    try:
        # Read the uploaded file
        data = pd.read_csv(file)
        
        # Extract IDs from URLs
        data['id'] = data['Top pages'].apply(extract_id_from_url)
        data = data.dropna(subset=['id'])
        data['id'] = data['id'].astype(int)

        # Database operations
        connection = get_db_connection()
        cursor = connection.cursor()

        # Check for existing IDs
        existing_ids_query = "SELECT id FROM NewTable"
        cursor.execute(existing_ids_query)
        existing_ids = set(row[0] for row in cursor.fetchall())

        # Filter out existing entries
        data = data[~data['id'].isin(existing_ids)]

        # Get BId values
        id_list = tuple(data['id'].tolist())
        if id_list:
            query = f"SELECT id, BId FROM TPHotel WHERE id IN ({','.join(['%s'] * len(id_list))})"
            cursor.execute(query, id_list)
            id_bid_map = {row[0]: row[1] for row in cursor.fetchall()}
            data['BId'] = data['id'].map(id_bid_map)
        else:
            data['BId'] = None

        # Add timestamp
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data['Updated_at'] = current_time

        # Create table if not exists
        create_table_query = """
        CREATE TABLE IF NOT EXISTS NewTable (
            URL TEXT,
            id INT UNIQUE,
            BId INT,
            Updated_at DATETIME
        )
        """
        cursor.execute(create_table_query)

        # Insert data
        insert_query = "INSERT IGNORE INTO NewTable (URL, id, BId, Updated_at) VALUES (%s, %s, %s, %s)"
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data.iloc[i:i+batch_size]
            cursor.executemany(insert_query, batch[['Top pages', 'id', 'BId', 'Updated_at']].values.tolist())

        # Remove duplicates
        remove_duplicates_query = """
        DELETE t1 FROM NewTable t1
        INNER JOIN NewTable t2 
        WHERE t1.id = t2.id AND t1.Updated_at < t2.Updated_at
        """
        cursor.execute(remove_duplicates_query)
        
        connection.commit()
        cursor.close()
        connection.close()

        return True, "File processed successfully!"
    except Exception as e:
        return False, f"Error processing file: {str(e)}"

def generate_null_bid_report():
    connection = get_db_connection()
    cursor = connection.cursor()
    
    fetch_query = """
    SELECT NewTable.URL, TPHotel.id as hotelid, TPHotel.name, TPHotel.address, TPHotel.CityName, TPHotel.CountryName 
    FROM NewTable 
    JOIN TPHotel ON NewTable.id = TPHotel.id 
    WHERE NewTable.BId IS NULL
    """
    cursor.execute(fetch_query)
    null_bids = cursor.fetchall()
    
    # Create temporary file
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx')
    null_bids_df = pd.DataFrame(null_bids, columns=['URL', 'Hotel ID', 'Hotel Name', 'Address', 'City Name', 'Country Name'])
    null_bids_df.to_excel(temp_file.name, index=False)
    
    cursor.close()
    connection.close()
    
    return temp_file.name

@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file selected')
            return redirect(request.url)
        
        file = request.files['file']
        if file.filename == '':
            flash('No file selected')
            return redirect(request.url)
        
        if file and file.filename.endswith(('.csv', '.xlsx')):
            success, message = process_excel_file(file)
            flash(message)
            return redirect(url_for('home'))
        else:
            flash('Please upload a valid Excel or CSV file')
            return redirect(request.url)
    
    return render_template('index.html')

@app.route('/download-report')
def download_report():
    try:
        report_path = generate_null_bid_report()
        return send_file(
            report_path,
            as_attachment=True,
            download_name='Null_BIds_Report.xlsx',
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        flash(f'Error generating report: {str(e)}')
        return redirect(url_for('home'))

if __name__ == '__main__':
    app.run(debug=True)
