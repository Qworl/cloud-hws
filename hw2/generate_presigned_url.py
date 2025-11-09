from decouple import config
import boto3

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

url = s3.generate_presigned_url(
    'get_object',
    Params={
        'Bucket': bucket_name,
        'Key': 'index.html',
        'VersionId': '0006432B019859D8'
    },
    ExpiresIn=3600  # срок в секундах
)

print(url)