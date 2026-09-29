import os
import time
import json
import pika
import mysql.connector
from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional

app = FastAPI()

DB_HOST = os.getenv("DB_HOST", "db")
DB_USER = os.getenv("DB_USER", "user")
DB_PASS = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "pagos_db")
RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")

def get_db_connection():
    while True:
        try:
            conn = mysql.connector.connect(
                host=DB_HOST,
                user=DB_USER,
                password=DB_PASS,
                database=DB_NAME
            )
            return conn
        except mysql.connector.Error:
            time.sleep(2)

def get_rabbit_channel():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
            channel = connection.channel()
            channel.queue_declare(queue='pagos_queue', durable=True)
            return connection, channel
        except pika.exceptions.AMQPConnectionError:
            time.sleep(2)

class PagoCreate(BaseModel):
    referencia: str
    valor: float = Field(..., gt=0)
    medio: str

@app.post("/pagos")
def registrar_pago(pago: PagoCreate):
    if not pago.referencia or not pago.medio or pago.valor <= 0:
        return {"status": False, "message": "Datos del pago inválidos"}

    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = "INSERT INTO pagos (referencia, valor, medio, estado) VALUES (%s, %s, %s, %s)"
    cursor.execute(query, (pago.referencia, pago.valor, pago.medio, 'REGISTRADO'))
    conn.commit()
    pago_id = cursor.lastrowid
    
    cursor.close()
    conn.close()

    rabbit_conn, channel = get_rabbit_channel()
    message = json.dumps({"pago_id": pago_id})
    channel.basic_publish(
        exchange='',
        routing_key='pagos_queue',
        body=message,
        properties=pika.BasicProperties(delivery_mode=2) # Mensaje persistente
    )
    rabbit_conn.close()

    return {
        "status": True,
        "message": "Pago registrado",
        "data": {
            "id": pago_id,
            "estado": "REGISTRADO"
        }
    }

@app.get("/pagos/{pago_id}")
def consultar_pago(pago_id: int):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT id, referencia, valor, medio, fecha_registro, estado FROM pagos WHERE id = %s", (pago_id,))
    pago = cursor.fetchone()
    
    cursor.close()
    conn.close()

    if not pago:
        raise HTTPException(status_code=404, detail="Pago no encontrado")

    return {"status": True, "data": pago}


@app.exception_handler(RequestValidationError)
async def validation_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=200,
        content={"status": False, "message": "Datos del pago inválidos"}
    )