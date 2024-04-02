FROM python:3.12
# Or any preferred Python version.
ADD main.py .
RUN pip install requests click google-cloud-storage google-cloud-secret-manager google-cloud-bigquery Jinja2
ENTRYPOINT [ "python", "./main.py"] 
# Or enter the name of your unique directory and parameter set.