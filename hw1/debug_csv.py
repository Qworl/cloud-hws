import csv

with open('moscow_cental_diameter.csv', 'r', encoding='utf-8') as f:
    lines = f.readlines()
    reader = csv.reader(lines[2:3], delimiter=';')
    row = next(reader)
    print('Row length:', len(row))
    print('Geodata column (index 18):', repr(row[18]))
    print('Startswith check:', row[18].startswith('"{coordinates='))