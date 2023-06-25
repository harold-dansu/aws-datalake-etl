FROM amazon/aws-lambda-python:3.7

RUN yum update -y && \
    yum install -y python3 python3-dev python3-pip gcc && \
    rm -Rf /var/cache/yum

COPY downloader.py ./

COPY requirements.txt ./

RUN pip install -r requirements.txt

CMD ["downloader.lambda_handler"]
