import mysql.connector
import os
import csv
from datetime import datetime, date
import logging

# Database connection parameters
DB_HOST = 'KaTuripu'
DB_NAME = 'achraf'
DB_USER = 'Ajrhourh'
DB_PASS = 'Put your password'

# Directory containing the CSV files
directory = 'D:/Yadine/Studs/MSBDA/4th Semester/Data Engineering/Projects/1/sales_csv/CSVs'

# Set up logging
logging.basicConfig(filename='integration.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')

# Connect to the database
try:
    conn = mysql.connector.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    cur = conn.cursor()
    logging.info('Database connection established.')
except Exception as e:
    logging.error(f'Error connecting to database: {str(e)}')
    raise

# Iterate over all the CSV files in the directory
for filename in os.listdir(directory):
    if filename.endswith(".csv"):
            # Open the file and read its contents
            with open(os.path.join(directory, filename), 'r') as f:
                reader = csv.DictReader(f)
                # Iterate over each row in the file and insert it into the database
                for row in reader:
                    # Transform the numeric fields to numeric format
                    try:
                        row['Units Sold'] = round(float(row['Units Sold'].replace('$', '').replace(',', '')), 2)
                        row['Unit Price'] = round(float(row['Unit Price'].replace('$', '').replace(',', '')), 2)
                        row['Unit Cost'] = round(float(row['Unit Cost'].replace('$', '').replace(',', '')), 2)
                        row['Total Revenue'] = round(float(row['Total Revenue'].replace('$', '').replace(',', '')), 2)
                        row['Total Cost'] = round(float(row['Total Cost'].replace('$', '').replace(',', '')), 2)
                        row['Total Profit'] = round(float(row['Total Profit'].replace('$', '').replace(',', '')), 2)
                    except ValueError as e:
                        # If a value error occurs during numeric conversion, log the error message and continue to the next row
                        logging.error(f'Error converting numeric values for row with ID {row["ID"]}: {e}')
                        continue
                        # Transform the date fields to date format
                    try:    
                        row['Order Date'] = datetime.strptime(row['Order Date'], '%m/%d/%Y').date()
                        row['Ship Date'] = datetime.strptime(row['Ship Date'], '%m/%d/%Y').date()
                    except ValueError as e:
                        # If a value error occurs during date conversion, log the error message and continue to the next row
                        logging.error(f'Error converting date values for row with ID {row["ID"]}: {e}')
                        continue
                    # Map 'Online' to 1 and 'Offline' to 0 for 'Sales Channel'
                    row['Sales Channel'] = 1 if row['Sales Channel'] == 'Online' else 0
                    try:    
                        # Insert the row into the database
                        cur.execute("""
                            INSERT INTO sales (
                                ID,
                                Region,
                                Country,
                                `Item Type`,
                                `Sales Channel`,
                                `Order Priority`,
                                `Order Date`,
                                `Order ID`,
                                `Ship Date`,
                                `Units Sold`,
                                `Unit Price`,
                                `Unit Cost`,
                                `Total Revenue`,
                                `Total Cost`,
                                `Total Profit`
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            int(row['ID']),
                            row['Region'],
                            row['Country'],
                            row['Item Type'],
                            row['Sales Channel'],
                            row['Order Priority'],
                            row['Order Date'],
                            int(row['Order ID']),
                            row['Ship Date'],
                            int(row['Units Sold']),
                            row['Unit Price'],
                            row['Unit Cost'],
                            row['Total Revenue'],
                            row['Total Cost'],
                            row['Total Profit']
                        ))
                        conn.commit()
                        logging.info(f'Successful insertion of row with ID {row["ID"]} into the database')

                    except Exception as e:
    # Rollback the transaction and log the error message
                        logging.error(f'Error inserting row with ID {row["ID"]} into the database: {e}')
                        continue

#Close the database connection
cur.close()
conn.close()

#Log successful completion of script
logging.info("Data integration process completed successfully.")
