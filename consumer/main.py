import os
import time
import json
import datetime
import pika
import mysql.connector

DB_HOST = os.getenv("DB_HOST", "db")
DB_USER = os.getenv("DB_USER", "user")
DB_PASS = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "pagos_db")
RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
PROCESSING_DELAY = int(os.getenv("PROCESSING_DELAY", "4"))
FAIL_ON_PROCESS = os.getenv("FAIL_ON_PROCESS", "false").lower() == "true"


def ahora():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def get_db_connection():
    while True:
        try:
            return mysql.connector.connect(
                host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME
            )
        except mysql.connector.Error:
            print(f"[{ahora()}] Esperando conexión a MySQL...", flush=True)
            time.sleep(2)


def process_payment(ch, method, properties, body):
    try:
        pago_id = json.loads(body).get("pago_id")
    except Exception:
        print(f"[{ahora()}] [ERROR] Mensaje mal formado, se descarta", flush=True)
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
        return

    print(f"[{ahora()}] [Consumidor] Tomado de la cola el pago ID: {pago_id}", flush=True)

    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        time.sleep(PROCESSING_DELAY)

        if FAIL_ON_PROCESS:
            raise RuntimeError("Fallo simulado durante el procesamiento")

        cursor.execute(
            "UPDATE pagos SET estado = 'PROCESADO' WHERE id = %s AND estado = 'REGISTRADO'",
            (pago_id,),
        )
        if cursor.rowcount == 1:
            cursor.execute(
                "INSERT INTO procesamientos (pago_id, resultado) VALUES (%s, %s)",
                (pago_id, f"Comprobante generado exitosamente en {PROCESSING_DELAY}s"),
            )
        conn.commit()

        print(f"[{ahora()}] [Consumidor] Finalizado procesamiento del pago ID: {pago_id}", flush=True)
        ch.basic_ack(delivery_tag=method.delivery_tag)  

    except Exception as e:
        print(f"[{ahora()}] [ERROR] Fallo al procesar pago ID {pago_id}: {e}", flush=True)
        conn.rollback()
        time.sleep(3)  
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    finally:
        cursor.close()
        conn.close()


def main():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
            channel = connection.channel()
            channel.queue_declare(queue="pagos_queue", durable=True)
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue="pagos_queue", on_message_callback=process_payment)
            print(f"[{ahora()}] [Consumidor] Esperando mensajes en la cola.", flush=True)
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            print(f"[{ahora()}] Esperando conexión a RabbitMQ...", flush=True)
            time.sleep(2)


if __name__ == "__main__":
    main()