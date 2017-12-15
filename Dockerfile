FROM debian:latest

# Set default environment variables
ENV HEALTHCHECK_PORT=5000
ENV FTP_PORT=2121
ENV PASSIVE_PORT_LOWER=60000
ENV PASSIVE_PORT_UPPER=60009

EXPOSE ${HEALTHCHECK_PORT}
EXPOSE ${FTP_PORT}

# Install dependencies
RUN apt-get update && apt-get install -y python3 python3-pip

# Add files to image
RUN mkdir /ftp-to-s3
ADD . /ftp-to-s3
WORKDIR /ftp-to-s3
RUN pip3 install --upgrade -r requirements.txt

CMD python3 server.py
