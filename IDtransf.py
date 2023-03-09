import csv

# Define the input and output file names
input_file = "sales_records_n5.csv"
output_file = "sales_records_n55.csv"

# Define the ID range for the new values
new_id_start = 400000
new_id_end = 499999

with open(input_file, "r") as infile, open(output_file, "w", newline="") as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile)

    # Write the header row to the output file
    header = next(reader)
    writer.writerow(header)

    # Iterate through each row in the input file (starting from row 2)
    for row in reader:
        # Get the current ID value from the row (if it exists)
        try:
            current_id = int(row[0])
        except ValueError:
            continue  # Skip this row if the ID value is not an integer
        # Check if the current ID is within the old range
        if current_id >= 0 and current_id <= 99999:
            # Calculate the new ID value
            new_id = current_id + new_id_start - 1
            # Update the ID value in the row
            row[0] = str(new_id)
        # Write the updated row to the output file
        writer.writerow(row)