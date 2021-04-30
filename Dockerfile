FROM python:3.9-slim
LABEL autor="Pavel Maksimov"
LABEL maintainer="vur21@ya.ru"

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

ENV FLOWMASTER_HOME="/usr/src/FlowMaster"
WORKDIR /usr/src/fm
COPY . /usr/src/fm
RUN python setup.py install
WORKDIR /usr/src/FlowMaster
CMD flowmaster run
