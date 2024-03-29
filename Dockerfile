FROM ubuntu:20.04
ENV TZ=Europe/Greece
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app/base
RUN apt-get update && \
	apt-get install -y \
		python3  \
		wget \
		git \
		python3-pip


COPY requirements.txt /app/base/requirements.txt
RUN pip install -r requirements.txt
RUN rm requirements.txt

COPY /communication /app/base/communication
COPY node.py /app/base/node.py
