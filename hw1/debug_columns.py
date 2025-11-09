import csv

# Function to debug column indices
def debug_columns(input_file):
    """
    Debug the column indices in the CSV file
    """
    with open(input_file, 'r', encoding='utf-8') as infile:
        # Read all lines
        lines = infile.readlines()
        
        # Process header lines (first two lines)
        for i in range(2):
            # Split the header line by delimiter
            header_row = lines[i].strip().split(';')
            print(f"Header line {i+1} has {len(header_row)} columns:")
            for j, col in enumerate(header_row):
                print(f"  Column {j}: {col}")
            print()

# Debug the original CSV file
print("=== Original CSV File ===")
debug_columns('moscow_cental_diameter.csv')

# Debug the filtered CSV file
print("=== Filtered CSV File ===")
debug_columns('moscow_cental_diameter_filtered.csv')