import zipfile
import os

# Define the .whl file and target folder
whl_file = "numpy-2.2.1-cp313-cp313-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"

target_folder = "C:/Users/pablo/Downloads/python"  # Use forward slashes

# Create the target folder
os.makedirs(target_folder, exist_ok=True)

# Extract the .whl file
with zipfile.ZipFile(whl_file, 'r') as zip_ref:
    zip_ref.extractall(target_folder)
