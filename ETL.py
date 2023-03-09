import psycopg2
import os
import csv
from datetime import datetime

# Database connection parameters
DB_HOST = 'localhost'
DB_NAME = 'project1'
DB_USER = 'postgres'
DB_PASS = 'Afoaieafoaie1'

# Directory containing the CSV files
directory = 'D:/Yadine/Studs/MSBDA/4th Semester/Data Engineering/Projects/1/sales_csv/CSVs'

# Connect to the database
conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
cur = conn.cursor()

# Iterate over all the CSV files in the directory
for filename in os.listdir(directory):
    if filename.endswith(".csv"):
        # Open the file and read its contents
        with open(os.path.join(directory, filename), 'r') as f:
            reader = csv.DictReader(f)
            # Iterate over each row in the file and insert it into the database
            for row in reader:
                # Transform the numeric fields to numeric format
                row['Units Sold'] = round(float(row['Units Sold'].replace('$', '').replace(',', '')), 2)
                row['Unit Price'] = round(float(row['Unit Price'].replace('$', '').replace(',', '')), 2)
                row['Unit Cost'] = round(float(row['Unit Cost'].replace('$', '').replace(',', '')), 2)
                row['Total Revenue'] = round(float(row['Total Revenue'].replace('$', '').replace(',', '')), 2)
                row['Total Cost'] = round(float(row['Total Cost'].replace('$', '').replace(',', '')), 2)
                row['Total Profit'] = round(float(row['Total Profit'].replace('$', '').replace(',', '')), 2)
                # Transform the date fields to date format
                row['Order Date'] = datetime.strptime(row['Order Date'], '%m/%d/%Y').date()
                row['Ship Date'] = datetime.strptime(row['Ship Date'], '%m/%d/%Y').date()
                # Insert the row into the database
                cur.execute("""
                    INSERT INTO sales (
                        ID,
                        Region,
                        Country,
                        "Item Type",
                        "Sales Channel",
                        "Order Priority",
                        "Order Date",
                        "Order ID",
                        "Ship Date",
                        "Units Sold",
                        "Unit Price",
                        "Unit Cost",
                        "Total Revenue",
                        "Total Cost",
                        "Total Profit"
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    int(row['ID']),
                    row['Region'],
                    row['Country'],
                    row['Item Type'],
                    row['Sales Channel'] == 'TRUE',
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

# Close the database connection
cur.close()
conn.close()
