import os
import requests
import zipfile
from io import BytesIO

def extract(url, extracted_dir):
    
    extracted_path = os.path.join(".", extracted_dir)
    if not os.path.exists(extracted_path):
        os.makedirs(extracted_path)
    response = requests.get(url)
    
    if response.status_code == 200:
        with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
            zip_ref.extractall(extracted_dir)
            print(f"Files extracted to {extracted_dir}")
    else:
        print(f"Failed to download the zip file. Status code: {response.status_code}")

    readme_path = os.path.join(extracted_dir, 'KETI', 'README.txt')
    sensors_zip_path = os.path.join(extracted_dir, 'sensors.zip')

    if os.path.exists(readme_path):
        os.remove(readme_path)
        print(f"Removed file: {readme_path}")
    else:
        print(f"File not found: {readme_path}")

    if os.path.exists(sensors_zip_path):
        os.remove(sensors_zip_path)
        print(f"Removed file: {sensors_zip_path}")
    else:
        print(f"File not found: {sensors_zip_path}")
