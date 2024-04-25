FROM python:3.12

RUN apt-get -y update
RUN apt-get install -y nodejs npm
RUN npm config set strict-ssl false
RUN npm install @salesforce/cli --global
RUN apt-get install -y jq

WORKDIR /app
ADD . .
RUN pip install -e .
WORKDIR /env
ENTRYPOINT ["talk-to-salesforce"] 