FROM python:3.12

WORKDIR /app                                          
COPY  requirements.txt /app/requirements.txt 

RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

COPY . /app

#CMD bash -c "while true; do dleep 1; done"
CMD ["python", "/app/Reporte InfluxDB V2_6.py"]
#ENTRYPOINT python main.py
