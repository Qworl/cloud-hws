# Test script to verify column removal
original_columns = [
    "StationName",           # 0
    "TransliterationStation", # 1
    "DiameterName",          # 2
    "City",                  # 3
    "District",              # 4
    "Area",                  # 5
    "Tariff",                # 6
    "Platforms",             # 7
    "MaskStation",           # 8
    "TransferMCD",           # 9
    "TransferAeroExpress",   # 10
    "TransferMetroStation",  # 11 - REMOVE
    "AeroexpressStation",    # 12
    "RailwayStation",        # 13
    "ExitTrainStations",     # 14 - REMOVE
    "WorkingHours",          # 15 - REMOVE
    "ObjectStatus",          # 16
    "global_id",             # 17
    "geoData",               # 18 - Transform
    "geodata_center",        # 19 - REMOVE
    ""                       # 20 - REMOVE (trailing empty column)
]

# Columns to remove (0-indexed)
columns_to_remove = [20, 19, 15, 14, 11]  # Remove in descending order

# Create filtered column list
filtered_columns = [col for i, col in enumerate(original_columns) if i not in columns_to_remove]

print("Original columns:")
for i, col in enumerate(original_columns):
    print(f"  {i}: {col}")

print("\nFiltered columns:")
for i, col in enumerate(filtered_columns):
    print(f"  {i}: {col}")

print(f"\nTotal original columns: {len(original_columns)}")
print(f"Total filtered columns: {len(filtered_columns)}")