import psycopg2
import csv

# Подключение к БД
conn = psycopg2.connect(
    host="address",
    port=6432,
    sslmode="verify-full",
    dbname="hw1",
    user="miafrolov",
    password="password",
    target_session_attrs="read-write"
)
cursor = conn.cursor()

def transform_geodata(geodata_str, is_multipoint=False):
    """
    Transform geodata string to the format "[latitude, longitude]" or list of "[lat, lon]" for MultiPoint
    """
    try:
        if is_multipoint:
            # Extract coordinates from format: {coordinates=[[37.637625605, 55.774162283]], type=MultiPoint}
            # or: {coordinates=[[37.147459424, 55.990477974], [37.147826503, 55.989766659]], type=MultiPoint}
            start = geodata_str.find('[[')
            end = geodata_str.find(']]')
            if start != -1 and end != -1:
                coords_part = geodata_str[start+2:end]
                # Split by '], [' to get individual coordinate pairs
                coord_pairs = coords_part.split('], [')
                transformed_coords = []
                for pair in coord_pairs:
                    lon_lat = pair.split(',')
                    if len(lon_lat) == 2:
                        lon = lon_lat[0].strip()
                        lat = lon_lat[1].strip()
                        # Create the new format: "[55.774162283, 37.637625605]"
                        transformed_coords.append(f'[{lat}, {lon}]')
                    else:
                        print(f"Unexpected coordinate format: {pair}")
                        return [geodata_str]
                return transformed_coords
            else:
                print(f"Unexpected MultiPoint format: {geodata_str}")
                return [geodata_str]
        else:
            # Extract coordinates from format: {coordinates=[37.281532, 55.672189], type=Point}
            start = geodata_str.find('[')
            end = geodata_str.find(']')
            if start != -1 and end != -1 and end > start:
                coords_part = geodata_str[start+1:end]
                lon_lat = coords_part.split(',')
                if len(lon_lat) == 2:
                    lon = lon_lat[0].strip()
                    lat = lon_lat[1].strip()
                    # Create the new format: "[55.672189, 37.281532]"
                    return f'[{lat}, {lon}]'
                else:
                    print(f"Unexpected Point coordinate format: {coords_part}")
                    return geodata_str
            else:
                print(f"Unexpected Point format: {geodata_str}")
                return geodata_str
    except Exception as e:
        print(f"Error transforming geodata: {e}")
        return geodata_str

def create_filtered_csv(input_file, output_file, columns_to_remove=None, geodata_columns=None, filter_nested_data=False):
    """
    Create a new CSV file with filtered columns and transformed geodata
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
        columns_to_remove: List of column indices to remove (0-indexed, in descending order)
        geodata_columns: Dictionary with column indices and is_multipoint flag for geodata transformation
        filter_nested_data: Boolean flag to filter out "nested data" values
    """
    if columns_to_remove is None:
        columns_to_remove = []
    
    if geodata_columns is None:
        geodata_columns = {}
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
         
        # Read all lines
        lines = infile.readlines()
        
        # Process header lines (first two lines)
        writer = csv.writer(outfile, delimiter=',', quoting=csv.QUOTE_ALL)
        for i in range(2):
            # Split the header line by delimiter
            header_row = lines[i].strip().split(';')
            # Remove specified columns in descending order
            if columns_to_remove:
                filtered_header_row = [header_row[j] for j in range(len(header_row)) if j not in columns_to_remove]
            else:
                filtered_header_row = header_row
            
            # Rename "geoData" to "geodata" in both header rows
            filtered_header_row = [col.replace('geoData', 'geodata') for col in filtered_header_row]
            
            # Write the filtered header line
            writer.writerow(filtered_header_row)
        
        # Process data rows (starting from line 3)
        reader = csv.reader(lines[2:], delimiter=';')
        
        for row in reader:
            # Transform geodata columns if present
            new_rows = [row]  # Start with the original row
            for col_index, is_multipoint in geodata_columns.items():
                if len(row) > col_index and row[col_index].startswith('{coordinates='):
                    transformed_geodata = transform_geodata(row[col_index], is_multipoint)
                    if is_multipoint and isinstance(transformed_geodata, list):
                        # For MultiPoint, create separate rows for each coordinate pair
                        new_rows = []
                        for i, geodata in enumerate(transformed_geodata):
                            new_row = row.copy()
                            new_row[col_index] = geodata
                            new_rows.append(new_row)
                    else:
                        # For Point or non-MultiPoint, just update the geodata in all rows
                        for new_row in new_rows:
                            new_row[col_index] = transformed_geodata
            
            # Process each new row
            for new_row in new_rows:
                # Filter out "nested data" values if requested
                if filter_nested_data:
                    for j in range(len(new_row)):
                        if new_row[j] == "nested data":
                            new_row[j] = ""
                
                # Remove specified columns in descending order to maintain correct indices
                if columns_to_remove:
                    filtered_row = [new_row[j] for j in range(len(new_row)) if j not in columns_to_remove]
                else:
                    filtered_row = new_row
                
                # Write this row to output
                writer.writerow(filtered_row)
        
# Create the filtered CSV files
print("Creating filtered CSV files...")

# For MCD data
mcd_columns_to_remove = [20, 19, 15, 14, 11]  # trailing empty column, geodata_center, WorkingHours, ExitTrainStations, and TransferMetroStation column
mcd_geodata_columns = {18: False}  # geodata column (0-indexed), False for Point type
create_filtered_csv('moscow_cental_diameter.csv', 'moscow_cental_diameter_filtered.csv',
                    mcd_columns_to_remove, mcd_geodata_columns)

# For Morgs data
# Remove columns: trailing empty column (33), geodata_center (32), and other columns with mostly empty/nested data
# Based on the data inspection, we'll remove columns that are mostly empty or contain "nested data"
# Keep important columns: Category(0), FullName(1), ShortName(2), ChiefName(5), ChiefGender(7), CloseFlag(12), CloseDate(14), ReopenDate(15), BedSpace(16), PaidServiceInfo(17), global_id(22), AmbulanceStationPhone(25), DrugStoreType(30), geoData(31)
morgs_columns_to_remove = [33, 32, 29, 28, 27, 26, 24, 23, 21, 20, 19, 18, 13, 11, 10, 9, 8, 6, 4, 3]  # Remove in descending order
morgs_geodata_columns = {31: True}  # geodata column (0-indexed), True for MultiPoint type
create_filtered_csv('morgs.csv', 'morgs_filtered.csv',
                    morgs_columns_to_remove, morgs_geodata_columns, filter_nested_data=True)
# For Roddom data (Maternity hospitals)
# Remove columns: trailing empty column (31), geodata_center (30), and other columns with mostly empty/nested data
# Keep important columns: Category(0), FullName(1), ShortName(2), ChiefName(5), ChiefGender(7), ChiefPhone(8),
# PublicPhone(9), Email(11), CloseFlag(12), CloseDate(14), ReopenDate(15), BedSpace(16), PaidServiceInfo(17),
# WorkingHours(18), Specialization(20), global_id(24), AmbulanceStation(23), AmbulanceStationPhone(25),
# DrugStoreType(29), geoData(30)
roddom_columns_to_remove = [32, 31, 28, 27, 26, 22, 21, 19, 13, 10, 6, 4, 3]  # Remove in descending order
roddom_geodata_columns = {30: True}  # geodata column (0-indexed), True for MultiPoint type
create_filtered_csv('roddom.csv', 'roddom_filtered.csv',
                    roddom_columns_to_remove, roddom_geodata_columns, filter_nested_data=True)

# For Clinic data
# Remove columns: trailing empty column (31), geodata_center (30), and other columns with mostly empty/nested data
# Keep important columns: Category(0), FullName(1), ShortName(2), ChiefName(5), ChiefGender(7), ChiefPhone(8), 
# PublicPhone(9), Email(11), CloseFlag(12), CloseDate(14), ReopenDate(15), BedSpace(16), PaidServiceInfo(17), 
# WorkingHours(18), Specialization(20), global_id(23), AmbulanceStation(24), AmbulanceStationPhone(25), 
# DrugStoreType(29), geoData(30)
clinic_columns_to_remove = [32, 31, 28, 27, 26, 22, 21, 19, 13, 10, 6, 4, 3]  # Remove in descending order
clinic_geodata_columns = {30: True}  # geodata column (0-indexed), True for MultiPoint type
create_filtered_csv('clinic.csv', 'clinic_filtered.csv',
                    clinic_columns_to_remove, clinic_geodata_columns, filter_nested_data=True)

# For Ambulance data
# Remove columns: trailing empty column (31), geodata_center (30), and other columns with mostly empty/nested data
# Keep important columns: Category(0), FullName(1), ShortName(2), ChiefName(5), ChiefGender(7), ChiefPhone(8),
# PublicPhone(9), Email(11), CloseFlag(12), CloseDate(14), ReopenDate(15), BedSpace(16), PaidServiceInfo(17),
# WorkingHours(18), global_id(19), Specialization(21), AmbulanceStation(24), AmbulanceStationPhone(25),
# DrugStoreType(29), geoData(30)
ambulance_columns_to_remove = [32, 31, 28, 27, 26, 23, 22, 20, 13, 10, 6, 4, 3]  # Remove in descending order
ambulance_geodata_columns = {30: True}  # geodata column (0-indexed), True for MultiPoint type
create_filtered_csv('ambulance.csv', 'ambulance_filtered.csv',
                    ambulance_columns_to_remove, ambulance_geodata_columns, filter_nested_data=True)

sql_mcd = """
COPY mcd_stations(
    station_name,
    transliteration_station,
    diameter_name,
    city,
    district,
    area,
    tariff,
    platforms,
    mask_station,
    transfer_mcd,
    transfer_aeroexpress,
    aeroexpress_station,
    railway_station,
    object_status,
    global_id,
    geodata)
FROM STDIN
WITH (
    FORMAT CSV,
    HEADER false,
    DELIMITER ','
)
"""

sql_roddom = """
COPY roddoms(
    category,
    full_name,
    short_name,
    chief_name,
    chief_gender,
    chief_phone,
    public_phone,
    email,
    close_flag,
    close_date,
    reopen_date,
    bed_space,
    paid_service_info,
    working_hours,
    specialization,
    ambulance_station,
    global_id,
    ambulance_station_phone,
    drug_store_type,
    geodata)
FROM STDIN
WITH (
    FORMAT CSV,
    HEADER false,
    DELIMITER ','
)
"""

sql_clinic = """
COPY clinics(
    category,
    full_name,
    short_name,
    chief_name,
    chief_gender,
    chief_phone,
    public_phone,
    email,
    close_flag,
    close_date,
    reopen_date,
    bed_space,
    paid_service_info,
    working_hours,
    specialization,
    global_id,
    ambulance_station,
    ambulance_station_phone,
    drug_store_type,
    geodata)
FROM STDIN
WITH (
    FORMAT CSV,
    HEADER false,
    DELIMITER ','
)
"""

sql_ambulance = """
COPY ambulance_stations(
    category,
    full_name,
    short_name,
    chief_name,
    chief_gender,
    chief_phone,
    public_phone,
    email,
    close_flag,
    close_date,
    reopen_date,
    bed_space,
    paid_service_info,
    working_hours,
    global_id,
    specialization,
    ambulance_station,
    ambulance_station_phone,
    drug_store_type,
    geodata)
FROM STDIN
WITH (
    FORMAT CSV,
    HEADER false,
    DELIMITER ','
)
"""

sql_morgs = """
COPY morgs(
    category,
    full_name,
    short_name,
    chief_name,
    chief_gender,
    close_flag,
    close_date,
    reopen_date,
    bed_space,
    paid_service_info,
    global_id,
    ambulance_station_phone,
    drug_store_type,
    geodata)
FROM STDIN
WITH (
    FORMAT CSV,
    HEADER false,
    DELIMITER ','
)
"""

try:
    cursor.execute("DROP TABLE mcd_stations;")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS mcd_stations (
            station_name TEXT,
            transliteration_station TEXT,
            diameter_name TEXT,
            city TEXT,
            district TEXT,
            area TEXT,
            tariff TEXT,
            platforms INTEGER,
            mask_station TEXT,
            transfer_mcd TEXT,
            transfer_aeroexpress TEXT,
            aeroexpress_station TEXT,
            railway_station TEXT,
            object_status TEXT,
            global_id BIGINT,
            geodata TEXT
        )
        """
    )

    cursor.execute("DROP TABLE morgs;")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS morgs (
            category TEXT,
            full_name TEXT,
            short_name TEXT,
            chief_name TEXT,
            chief_gender TEXT,
            close_flag TEXT,
            close_date TEXT,
            reopen_date TEXT,
            bed_space TEXT,
            paid_service_info TEXT,
            global_id BIGINT,
            ambulance_station_phone TEXT,
            drug_store_type TEXT,
            geodata TEXT
        )
        """
    )

    cursor.execute("DROP TABLE roddoms;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS roddoms (
            category TEXT,
            full_name TEXT,
            short_name TEXT,
            chief_name TEXT,
            chief_gender TEXT,
            chief_phone TEXT,
            public_phone TEXT,
            email TEXT,
            close_flag TEXT,
            close_date TEXT,
            reopen_date TEXT,
            bed_space TEXT,
            paid_service_info TEXT,
            working_hours TEXT,
            specialization TEXT,
            global_id TEXT,
            ambulance_station TEXT,
            ambulance_station_phone TEXT,
            drug_store_type TEXT,
            geodata TEXT
        )
    """)
    
    cursor.execute("DROP TABLE clinics;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS clinics (
            category TEXT,
            full_name TEXT,
            short_name TEXT,
            chief_name TEXT,
            chief_gender TEXT,
            chief_phone TEXT,
            public_phone TEXT,
            email TEXT,
            close_flag TEXT,
            close_date TEXT,
            reopen_date TEXT,
            bed_space TEXT,
            paid_service_info TEXT,
            working_hours TEXT,
            specialization TEXT,
            global_id TEXT,
            ambulance_station TEXT,
            ambulance_station_phone TEXT,
            drug_store_type TEXT,
            geodata TEXT
        )
    """)

    cursor.execute("DROP TABLE ambulance_stations;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ambulance_stations (
            category TEXT,
            full_name TEXT,
            short_name TEXT,
            chief_name TEXT,
            chief_gender TEXT,
            chief_phone TEXT,
            public_phone TEXT,
            email TEXT,
            close_flag TEXT,
            close_date TEXT,
            reopen_date TEXT,
            bed_space TEXT,
            paid_service_info TEXT,
            working_hours TEXT,
            global_id TEXT,
            specialization TEXT,
            ambulance_station TEXT,
            ambulance_station_phone TEXT,
            drug_store_type TEXT,
            geodata TEXT
        );
    """)
    
    with open('moscow_cental_diameter_filtered.csv', 'r', encoding='utf-8') as f:
        # Пропустить первые две строки с заголовками
        next(f)  # Пропустить русские заголовки
        next(f)  # Пропустить английские заголовки
        cursor.copy_expert(sql_mcd, f)
    
    # Load Morgs data
    with open('morgs_filtered.csv', 'r', encoding='utf-8') as f:
        # Пропустить первые две строки с заголовками
        next(f)  # Пропустить русские заголовки
        next(f)  # Пропустить английские заголовки
        cursor.copy_expert(sql_morgs, f)
    
    # Load Roddom data
    with open('roddom_filtered.csv', 'r', encoding='utf-8') as f:
        # Пропустить первые две строки с заголовками
        next(f)  # Пропустить русские заголовки
        next(f)  # Пропустить английские заголовки
        try:
            cursor.copy_expert(sql_roddom, f)
        except Exception as e:
            print(f"Error loading roddom data: {e}")
            raise
    
    # Load Clinic data
    with open('clinic_filtered.csv', 'r', encoding='utf-8') as f:
        # Пропустить первые две строки с заголовками
        next(f)  # Пропустить русские заголовки
        next(f)  # Пропустить английские заголовки
        cursor.copy_expert(sql_clinic, f)
    
    # Load Ambulance data
    with open('ambulance_filtered.csv', 'r', encoding='utf-8') as f:
        # Пропустить первые две строки с заголовками
        next(f)  # Пропустить русские заголовки
        next(f)  # Пропустить английские заголовки
        cursor.copy_expert(sql_ambulance, f)
    
    conn.commit()
    print("Данные успешно загружены!")
    
except Exception as e:
    conn.rollback()
    print(f"Ошибка при загрузке: {e}")
    
finally:
    cursor.close()
    conn.close()
    
    # Comment out cleanup for verification
    # import os
    # if os.path.exists('moscow_cental_diameter_filtered.csv'):
    #     os.remove('moscow_cental_diameter_filtered.csv')
    # if os.path.exists('morgs_filtered.csv'):
    #     os.remove('morgs_filtered.csv')
    print("Filtered CSV files saved for verification.")