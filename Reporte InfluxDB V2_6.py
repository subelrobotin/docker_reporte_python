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

# Descripciones para los motores
measurement_descriptions = {
    "Parámetros_Energía_P1": "Motor 1",
    "Parámetros_Energía_P4": "Motor 2",
    "Parámetros_Energía_P7": "Motor 3",
    "Parámetros_Energía_P10": "Motor 4",
    "Parámetros_Energía_P13": "Motor 5"
}

# Función para ejecutar consultas y procesar resultados
def execute_query(start_time, end_time):
    query = f'''
    from(bucket: "{bucket}")
        |> range(start: {start_time.isoformat()}, stop: {end_time.isoformat()})
        |> filter(fn: (r) => r["_measurement"] == "Parámetros_Energía_P1" or r["_measurement"] == "Parámetros_Energía_P4" or r["_measurement"] == "Parámetros_Energía_P7" or r["_measurement"] == "Parámetros_Energía_P10" or r["_measurement"] == "Parámetros_Energía_P13")
        |> filter(fn: (r) => r["_field"] == "USB0_N2_Export_kVArh" or r["_field"] == "USB0_N2_Import_kVArh" or r["_field"] == "USB0_N2_Export_kWh" or r["_field"] == "USB0_N2_Import_kWh")
    '''
    try:
        result = query_api.query(org=org, query=query)
        results = []
        for table in result:
            for record in table.records:
                utc_time = record.get_time()
                local_time = utc_time.astimezone(pytz.timezone('America/Lima'))
                formatted_time = local_time.strftime('%Y-%m-%d %H:%M')
                results.append((formatted_time, record.get_measurement(), record.get_field(), record.get_value()))
        return pd.DataFrame(results, columns=["Time", "Measurement", "Field", "Value"])
    except Exception as e:
        print(f"Error al ejecutar la consulta: {e}")
        return pd.DataFrame(columns=["Time", "Measurement", "Field", "Value"])

# Definir los periodos para los turnos
def definir_turnos():
    ahora = datetime.now(pytz.timezone('America/Lima'))
    return {
        "Turno 1 (6 AM a 6 PM)": (ahora.replace(hour=6, minute=0, second=0, microsecond=0) - timedelta(days=1), ahora.replace(hour=18, minute=0, second=0, microsecond=0) - timedelta(days=1)),
        "Turno 2 (6 PM a 10 PM)": (ahora.replace(hour=18, minute=0, second=0, microsecond=0) - timedelta(days=1), ahora.replace(hour=22, minute=0, second=0, microsecond=0) - timedelta(days=1)),
        "Turno 3 (10 PM a 6 AM)": (ahora.replace(hour=22, minute=0, second=0, microsecond=0) - timedelta(days=1), ahora.replace(hour=6, minute=0, second=0, microsecond=0))
    }

# Función para procesar los datos y enviar el correo
def process_and_send_email():
    turnos = definir_turnos()
    html_tables = ""
    
    # Obtener la fecha y hora actual para el encabezado
    fecha_envio = datetime.now(pytz.timezone('America/Lima')).strftime("%Y-%m-%d")
    Hora_envio = datetime.now(pytz.timezone('America/Lima')).strftime("%H:%M:%S")
    
    # Incluir la fecha de envío en el inicio del reporte
    html_tables += f"<h2>Reporte de Consumo de Energía</h2>"
    html_tables += f"<p>Fecha:{fecha_envio} Hora:{Hora_envio}</p>"

    # Iterar sobre los motores en orden
    for measurement, motor_desc in sorted(measurement_descriptions.items(), key=lambda x: x[1]):
        html_tables += f"<h2>{motor_desc}</h2>"
        
        for turno, (start_time, end_time) in turnos.items():
            df = execute_query(start_time, end_time)
            
            if df.empty:
                print(f"Sin datos para {turno} en {motor_desc}")
                continue
            
            df_motor = df[df['Measurement'] == measurement]
            
            if df_motor.empty:
                continue
            
            # Procesar resultados en una tabla
            summary_df = df_motor.groupby("Field").agg({
                "Value": ["max", "min"]
            }).reset_index()
            
            summary_df.columns = ["Tipo de Energía", "Valor Final", "Valor Inicial"]
            summary_df["Diferencia"] = summary_df["Valor Final"] - summary_df["Valor Inicial"]
            
            # Reemplazar nombres de campos
            summary_df['Tipo de Energía'] = summary_df['Tipo de Energía'].map(field_descriptions).fillna(summary_df['Tipo de Energía'])
            
            # Reordenar columnas
            summary_df = summary_df[["Tipo de Energía", "Valor Inicial", "Valor Final", "Diferencia"]]
            
            # Generar tabla HTML para el turno
            html_tables += f"<h3>{turno}</h3>"
            html_tables += summary_df.to_html(index=False, border=1, classes='dataframe', justify='center')

    if not html_tables:
        print("No hay datos para enviar en el correo.")
        return

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
                padding: 0 10px;
            }}
            .header img {{
                height: auto;
                max-height: 60px;
            }}
            .header .logo-subel {{
                max-width: 300px;
            }}
            .header .logo-alprosa {{
                max-width: 1000px;
                margin-left: auto;
            }}
            h2 {{
                text-align: center;
                color: #333333;
            }}
            h3 {{
                text-align: center;
                color: #555555;
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
                <img src="https://subel.com.pe/wp-content/uploads/2024/02/SUBEL-HORIZONTAL-768x228.jpg" class="logo-subel" alt="Logo Subel">
                <img src="https://kamaqibusiness.com.pe/wp-content/uploads/2022/06/banner-logo-2.png" class="logo-alprosa" alt="Logo Alprosa">
            </div>
            {html_tables}
        </div>
    </body>
    </html>
    """
    
    # Configurar y enviar el correo
    def send_email(subject, html_body, to):
        msg = MIMEMultipart("alternative")
        msg['Subject'] = subject
        msg['From'] = "subelrobotin@gmail.com"
        msg['To'] = to

        # Adjuntar el cuerpo del mensaje en formato HTML
        msg.attach(MIMEText(html_body, 'html'))

        # Enviar el correo
        try:
            with smtplib.SMTP('smtp.gmail.com', 587) as server:
                server.starttls()
                server.login("subelrobotin@gmail.com", "yvrdxrxzridojgwx")
                server.sendmail(msg['From'], msg['To'], msg.as_string())
            print("Correo enviado exitosamente.")
        except Exception as e:
            print(f"Error al enviar el correo: {e}")

    send_email("Reporte de Consumo de Energía", html_body, "subelrobotin@gmail.com")

# Programar el script para que se ejecute diariamente a las 6 AM
schedule.every().day.at("06:30").do(process_and_send_email)

# Mantener el script en ejecución para que el programador funcione
while True:
    schedule.run_pending()
    time.sleep(60)


