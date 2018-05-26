FROM ubuntu
LABEL NETID rc5
LABEL CLASS cs398
LABEL SCHOOL uiuc
RUN  apt-get update
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
run pip3 install --upgrade pip
run pip3 install flask

ADD info.txt ./data/
ADD app.py ./python/
CMD python3 ./python/app.py
