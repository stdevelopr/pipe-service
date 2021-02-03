FROM python:3.9

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt
RUN export FLASK_APP=app

COPY . .

EXPOSE 5000

CMD [ "python", "run.py" ]