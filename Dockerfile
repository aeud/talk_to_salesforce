FROM python:3.12
RUN apt-get -y update
RUN apt-get install -y nodejs npm
RUN npm config set strict-ssl false
WORKDIR /app
ADD dist dist
RUN pip install dist/talk_to_salesforce-0.3.tar.gz
RUN npm install @salesforce/cli --global
RUN apt-get install -y jq
WORKDIR /env
ENTRYPOINT ["talk-to-salesforce"] 