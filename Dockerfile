FROM ubuntu:20.04
ENV TZ=Europe/Greece
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app
RUN apt-get update
RUN apt-get install -y python3 wget git
RUN apt-get install -y  python3-pip
COPY /communication /app/communication
COPY node.py /app/node.py
COPY defaults /app/defaults
RUN pip install -r requirements.txt