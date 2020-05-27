FROM python:3.7.7-buster
EXPOSE 8085
ADD . /Assignment3
WORKDIR /Assignment3
RUN pip install Flask
RUN pip install requests
RUN pip install flask_restful
CMD python assignment3.py
