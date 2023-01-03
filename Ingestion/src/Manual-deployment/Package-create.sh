cd
cd newProject/aws-pipeline/venv/lib/python3.9/site-packages/
zip -r ../../../../UploadPackage55 . -x "boto*"
cd
cd newProject/aws-pipeline/Ingestion/src
zip -r ../../UploadPackage55 . -x "__pycache"

