FROM python:3.12
# Or any preferred Python version.
WORKDIR /app
ADD main.py .
ADD requirements.txt .
ADD src src
RUN pip install -r requirements.txt
ENTRYPOINT [ "python", "./main.py"] 
# Or enter the name of your unique directory and parameter set.