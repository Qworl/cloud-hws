from decouple import config
import boto3
from botocore.exceptions import ClientError

# Читаем переменные из .env
aws_access_key_id = config('AWS_ACCESS_KEY_ID')
aws_secret_access_key = config('AWS_SECRET_ACCESS_KEY')
bucket_name = config('BUCKET_NAME')

s3 = boto3.client(
    's3',
    endpoint_url='https://storage.yandexcloud.net',
    region_name='ru-central1',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)


def upload_csv_to_s3(local_file_path: str, s3_file_path: str):
    """
    Upload a CSV file to Yandex Cloud S3 storage.
    
    Args:
        local_file_path (str): Path to the local CSV file to upload
        s3_file_path (str): Path in S3 where the file should be stored
    
    Returns:
        bool: True if upload was successful, None otherwise
    
    Raises:
        FileNotFoundError: If the local file does not exist
        ClientError: If there is an error during S3 upload
    """
    try:
        s3.upload_file(
            local_file_path,
            bucket_name,
            s3_file_path,
            ExtraArgs={'ContentType': 'text/csv'}
        )
        return True
        
    except FileNotFoundError:
        print(f"Ошибка: файл {local_file_path} не найден")
        
    except ClientError as e:
        print(f"Ошибка при загрузке в S3: {e}")
    
upload_csv_to_s3(local_file_path='moscow_cental_diameter_filtered.csv', s3_file_path='datasets/moscow_cental_diameter_filtered.csv')
