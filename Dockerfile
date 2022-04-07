FROM ubuntu:20.04
ENV TZ=Europe/Greece
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update
RUN apt-get install -y python3 wget git
RUN apt-get install -y  python3-pip