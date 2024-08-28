import influxdb_client
import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from datetime import datetime, timedelta
import pytz
import schedule
import time

# Configuración de InfluxDB
bucket = "ME_SmartX835"
org = "subel_desarrollador"
token = "FwgYjYBPknZasV4Aez-rSIxY2WippDIqVwflll_xkRDxkQ9BL3cWM0B0bUHk05cioSS0u1KUzvvHXuhXLspJpA=="
url = "http://54.85.83.64:8086"

client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org,
)

query_api = client.query_api()

# Descripciones para los campos
field_descriptions = {
    "USB0_N2_Export_kVArh": "Energía Reactiva Exportada (kVArh)",
    "USB0_N2_Import_kVArh": "Energía Reactiva Importada (kVArh)",
    "USB0_N2_Export_kWh": "Energía Activa Exportada (kWh)",
    "USB0_N2_Import_kWh": "Energía Activa Importada (kWh)"
}

# Función para ejecutar consultas y procesar resultados
def execute_query(start_time, end_time):
    query = f'''
    from(bucket: "{bucket}")
        |> range(start: {start_time.isoformat()}, stop: {end_time.isoformat()})
        |> filter(fn: (r) => r["_measurement"] == "Parámetros_Energía_P1" or r["_measurement"] == "Parámetros_Energía_P4" or r["_measurement"] == "Parámetros_Energía_P7" or r["_measurement"] == "Parámetros_Energía_P10" or r["_measurement"] == "Parámetros_Energía_P13")
        |> filter(fn: (r) => r["_field"] == "USB0_N2_Export_kVArh" or r["_field"] == "USB0_N2_Import_kVArh" or r["_field"] == "USB0_N2_Export_kWh" or r["_field"] == "USB0_N2_Import_kWh")
    '''
    result = query_api.query(org=org, query=query)
    results = []
    for table in result:
        for record in table.records:
            utc_time = record.get_time()
            local_time = utc_time.astimezone(pytz.timezone('America/Lima'))
            formatted_time = local_time.strftime('%Y-%m-%d %H:%M')
            results.append((formatted_time, record.get_measurement(), record.get_field(), record.get_value()))
    return pd.DataFrame(results, columns=["Time", "Measurement", "Field", "Value"])

# Definir los periodos para los turnos
turnos = {
    "Turno 1 (6 AM a 6 PM)": (datetime.now(pytz.timezone('America/Lima')).replace(hour=6, minute=0, second=0, microsecond=0), datetime.now(pytz.timezone('America/Lima')).replace(hour=18, minute=0, second=0, microsecond=0) - timedelta(days=1)),
    "Turno 2 (6 PM a 10 PM)": (datetime.now(pytz.timezone('America/Lima')).replace(hour=18, minute=0, second=0, microsecond=0) - timedelta(days=1), datetime.now(pytz.timezone('America/Lima')).replace(hour=22, minute=0, second=0, microsecond=0) - timedelta(days=1)),
    "Turno 3 (10 PM a 6 AM)": (datetime.now(pytz.timezone('America/Lima')).replace(hour=22, minute=0, second=0, microsecond=0) - timedelta(days=1), datetime.now(pytz.timezone('America/Lima')).replace(hour=6, minute=0, second=0, microsecond=0))
}

# Función para procesar los datos y enviar el correo
def process_and_send_email():
    html_tables = ""
    for turno, (start_time, end_time) in turnos.items():
        df = execute_query(start_time, end_time)
        
        # Procesar resultados en una tabla
        summary_df = df.groupby("Field").agg({
            "Value": ["max", "min"]
        }).reset_index()
        
        summary_df.columns = ["Field", "Valor Máximo", "Valor Mínimo"]
        summary_df["Diferencia"] = summary_df["Valor Máximo"] - summary_df["Valor Mínimo"]
        
        # Reemplazar nombres de campos
        summary_df['Field'] = summary_df['Field'].map(field_descriptions).fillna(summary_df['Field'])
        
        # Generar tabla HTML
        html_tables += f"<h2>{turno}</h2>"
        html_tables += summary_df.to_html(index=False, border=1, classes='dataframe', justify='center')

    # Estilo y estructura del correo
    html_body = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 0;
                background-color: #f9f9f9;
            }}
            .container {{
                width: 100%;
                max-width: 800px;
                margin: 0 auto;
                padding: 20px;
                background-color: #ffffff;
                border-radius: 8px;
                box-shadow: 0px 0px 10px rgba(0, 0, 0, 0.1);
            }}
            .header {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 20px;
                padding: 0 10px; /* Añadir padding para separación */
            }}
            .header img {{
                height: auto;
                max-height: 60px;
            }}
            .header .logo-subel {{
                max-width: 300px; /* Ajusta el tamaño de la imagen de la empresa 1 */
            }}
            .header .logo-alprosa {{
                max-width: 1000px; /* Ajusta el tamaño de la imagen de la empresa 2 */
                margin-left: auto; /* Alinea la imagen a la derecha */
            }}
            h2 {{
                text-align: center;
                color: #333333;
            }}
            .dataframe {{
                margin-left: auto;
                margin-right: auto;
                border-collapse: collapse;
                width: 80%;
            }}
            .dataframe th, .dataframe td {{
                border: 1px solid black;
                text-align: center;
                padding: 8px;
            }}
            .dataframe th {{
                background-color: #f2f2f2;
            }}
            .dataframe td {{
                background-color: #ffffff;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <img class="logo-subel" src="https://subel.com.pe/wp-content/uploads/2024/02/SUBEL-HORIZONTAL-768x228.jpg" alt="Empresa 1">
                <img class="logo-alprosa" src="https://corporacioncervesur.com.pe/wp-content/uploads/2024/06/alprosa_logo.jpg" alt="Empresa 2">
            </div>
            <h2>Reporte de Consumo de Energía</h2>
            {html_tables}
        </div>
    </body>
    </html>
    """

    # Configurar y enviar el correo
    def send_email(subject, html_body, to):
        msg = MIMEMultipart("alternative")
        msg['Subject'] = subject
        msg['From'] = "josealfredogalarzasequeiros@gmail.com"
        msg['To'] = to

        # Adjuntar el cuerpo del mensaje en formato HTML
        msg.attach(MIMEText(html_body, 'html'))

        # Enviar el correo
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login("josealfredogalarzasequeiros@gmail.com", "qfcb optl yujp gtmz")  # Reemplaza con tu contraseña
            server.sendmail(msg['From'], msg['To'], msg.as_string())

    # Enviar el correo con los datos formateados
    send_email(
        subject="Informe de Consumo de Energía",
        html_body=html_body,
        to="subelrobotin@gmail.com"
    )

    print("Correo enviado con éxito.")

# Programar la tarea para que se ejecute a las 6 AM todos los días
schedule.every().day.at("06:00").do(process_and_send_email)

# Bucle principal para ejecutar las tareas programadas
while True:
    schedule.run_pending()
    time.sleep(60)  # Esperar un minuto antes de verificar si hay tareas pendientes


