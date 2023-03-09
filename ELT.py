import pandas as pd
import subprocess
import glob

# Define path to csv files
path_to_csv = "*.csv"

# Concatenate all csv files into a single dataframe
df = pd.concat([pd.read_csv(f) for f in glob.glob(path_to_csv)])

# Apply transformations to the dataframe
df['Order Date'] = pd.to_datetime(df['Order Date'])
df['Ship Date'] = pd.to_datetime(df['Ship Date'])

# Remove '$' sign and convert columns to numeric
df['Total Revenue'] = pd.to_numeric(df['Total Revenue'].str.replace('$', '').str.replace(',', '').str.strip())
df['Total Cost'] = pd.to_numeric(df['Total Cost'].str.replace('$', '').str.replace(',', '').str.strip())

# Add 'Total Profit' column
df['Total Profit'] = df['Total Revenue'] - df['Total Cost']

# Convert money columns to float
money_cols = ['Unit Price', 'Unit Cost', 'Total Revenue', 'Total Cost', 'Total Profit']
df[money_cols] = df[money_cols].replace('[\$,]', '', regex=True).astype(float)

# Write the transformed data to a temporary file
output = "/sales_transformed.csv"
df.to_csv(output, index=False)

print("Transformed data written to file successfully")

# Define Hadoop command
hadoop_cmd = [
    "hadoop",
    "jar",
    "hadoop-streaming.jar",
    "-files",
    "/mapper.py,/reducer.py",
    "-mapper",
    "python3 mapper.py",
    "-reducer",
    "python3 reducer.py",
    "-input",
    "hdfs://namenode:9870/sales_transformed.csv",
    "-output",
    "/sales_output"
]

# Run Hadoop job
subprocess.run(hadoop_cmd, check=True)

# Read output from Hadoop job
output = subprocess.check_output(["hadoop", "fs", "-cat", "/sales_output/part-00000"]).decode("utf-8")

# Print output
print(output)
