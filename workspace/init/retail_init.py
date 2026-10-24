"""Carga retail_db en mysql-retail: el origen operacional del ELT.

Mismo esquema que el retail_db clásico de los cursos de Hadoop (departments,
categories, products, customers, orders, order_items). Los datos se generan
con una semilla fija, así todos los alumnos tienen exactamente las mismas filas.

Si las tablas ya tienen datos no hace nada. Para recargar desde cero:
    docker compose -f docker-compose.stack.yml run --rm retail-init python /workspace/init/retail_init.py --force
"""

import os
import random
import sys
import time
from datetime import datetime, timedelta

import pymysql

HOST = os.getenv("RETAIL_MYSQL_HOST", "mysql-retail")
PORT = int(os.getenv("RETAIL_MYSQL_PORT", "3306"))
USER = os.getenv("RETAIL_MYSQL_USER", "retail")
PASSWORD = os.getenv("RETAIL_MYSQL_PASSWORD", "retail")
DATABASE = os.getenv("RETAIL_MYSQL_DATABASE", "retail_db")

NUM_CUSTOMERS = 2000
NUM_ORDERS = 20000
SEED = 2026

DDL = [
    "DROP TABLE IF EXISTS order_items",
    "DROP TABLE IF EXISTS orders",
    "DROP TABLE IF EXISTS customers",
    "DROP TABLE IF EXISTS products",
    "DROP TABLE IF EXISTS categories",
    "DROP TABLE IF EXISTS departments",
    """CREATE TABLE departments (
        department_id   INT PRIMARY KEY,
        department_name VARCHAR(45) NOT NULL
    )""",
    """CREATE TABLE categories (
        category_id            INT PRIMARY KEY,
        category_department_id INT NOT NULL,
        category_name          VARCHAR(45) NOT NULL,
        FOREIGN KEY (category_department_id) REFERENCES departments (department_id)
    )""",
    """CREATE TABLE products (
        product_id          INT PRIMARY KEY,
        product_category_id INT NOT NULL,
        product_name        VARCHAR(45) NOT NULL,
        product_description VARCHAR(255) NOT NULL,
        product_price       DECIMAL(10, 2) NOT NULL,
        product_image       VARCHAR(255) NOT NULL,
        FOREIGN KEY (product_category_id) REFERENCES categories (category_id)
    )""",
    """CREATE TABLE customers (
        customer_id       INT PRIMARY KEY,
        customer_fname    VARCHAR(45) NOT NULL,
        customer_lname    VARCHAR(45) NOT NULL,
        customer_email    VARCHAR(80) NOT NULL,
        customer_password VARCHAR(45) NOT NULL,
        customer_street   VARCHAR(255) NOT NULL,
        customer_city     VARCHAR(45) NOT NULL,
        customer_state    VARCHAR(45) NOT NULL,
        customer_zipcode  VARCHAR(10) NOT NULL
    )""",
    """CREATE TABLE orders (
        order_id          INT PRIMARY KEY,
        order_date        DATETIME NOT NULL,
        order_customer_id INT NOT NULL,
        order_status      VARCHAR(45) NOT NULL,
        FOREIGN KEY (order_customer_id) REFERENCES customers (customer_id)
    )""",
    """CREATE TABLE order_items (
        order_item_id            INT PRIMARY KEY,
        order_item_order_id      INT NOT NULL,
        order_item_product_id    INT NOT NULL,
        order_item_quantity      TINYINT NOT NULL,
        order_item_subtotal      DECIMAL(10, 2) NOT NULL,
        order_item_product_price DECIMAL(10, 2) NOT NULL,
        FOREIGN KEY (order_item_order_id) REFERENCES orders (order_id),
        FOREIGN KEY (order_item_product_id) REFERENCES products (product_id)
    )""",
]

CATALOGO = {
    "Deportes": ["Fútbol", "Running", "Ciclismo", "Natación", "Montaña"],
    "Hogar": ["Cocina", "Baño", "Jardín", "Iluminación"],
    "Electrónica": ["Portátiles", "Móviles", "Audio", "Fotografía", "Videojuegos"],
    "Moda": ["Hombre", "Mujer", "Infantil", "Calzado"],
    "Libros": ["Novela", "Informática", "Cómic"],
    "Alimentación": ["Gourmet", "Bebidas", "Ecológico"],
}

NOMBRES = ["Lucía", "Hugo", "Martina", "Mateo", "Sofía", "Leo", "Julia", "Daniel",
           "Paula", "Pablo", "Valeria", "Álvaro", "Emma", "Adrián", "Carla", "Manuel",
           "Sara", "Javier", "Elena", "Diego", "Laura", "Marcos", "Irene", "Sergio"]
APELLIDOS = ["García", "Fernández", "González", "Rodríguez", "López", "Martínez",
             "Sánchez", "Pérez", "Gómez", "Martín", "Ruiz", "Díaz", "Hernández",
             "Álvarez", "Moreno", "Muñoz", "Romero", "Navarro", "Torres", "Gutiérrez"]
CALLES = ["Calle Mayor", "Avenida de la Constitución", "Calle Real", "Paseo Marítimo",
          "Calle del Sol", "Avenida de Castilla", "Calle San Fernando", "Plaza de España"]
# (ciudad, provincia, prefijo postal, peso): las grandes ciudades tienen más clientes
CIUDADES = [
    ("Madrid", "Madrid", "28", 18), ("Barcelona", "Barcelona", "08", 15),
    ("Valencia", "Valencia", "46", 8), ("Sevilla", "Sevilla", "41", 7),
    ("Zaragoza", "Zaragoza", "50", 5), ("Málaga", "Málaga", "29", 5),
    ("Bilbao", "Bizkaia", "48", 4), ("Santander", "Cantabria", "39", 4),
    ("Torrelavega", "Cantabria", "39", 2), ("Valladolid", "Valladolid", "47", 3),
    ("Oviedo", "Asturias", "33", 3), ("A Coruña", "A Coruña", "15", 3),
    ("Murcia", "Murcia", "30", 3), ("Palma", "Illes Balears", "07", 3),
    ("Las Palmas", "Las Palmas", "35", 3), ("Granada", "Granada", "18", 2),
    ("Pamplona", "Navarra", "31", 2), ("Salamanca", "Salamanca", "37", 2),
]
ESTADOS = [("COMPLETE", 33), ("CLOSED", 11), ("PENDING_PAYMENT", 22), ("PROCESSING", 12),
           ("PENDING", 11), ("ON_HOLD", 5), ("PAYMENT_REVIEW", 1), ("CANCELED", 2),
           ("SUSPECTED_FRAUD", 3)]


def conectar():
    for intento in range(60):
        try:
            return pymysql.connect(host=HOST, port=PORT, user=USER, password=PASSWORD,
                                   database=DATABASE, charset="utf8mb4", autocommit=False)
        except pymysql.err.OperationalError as error:
            print(f"Esperando a {HOST}:{PORT} ({intento + 1}/60): {error}")
            time.sleep(2)
    sys.exit(f"No se pudo conectar a {HOST}:{PORT}")


def ya_cargada(cursor):
    cursor.execute("SELECT COUNT(*) FROM information_schema.tables "
                   "WHERE table_schema = %s AND table_name = 'order_items'", (DATABASE,))
    if cursor.fetchone()[0] == 0:
        return False
    cursor.execute("SELECT COUNT(*) FROM order_items")
    return cursor.fetchone()[0] > 0


def generar(rnd):
    departments, categories, products = [], [], []
    for dep_id, (departamento, categorias) in enumerate(CATALOGO.items(), start=1):
        departments.append((dep_id, departamento))
        for categoria in categorias:
            cat_id = len(categories) + 1
            categories.append((cat_id, dep_id, categoria))
            for _ in range(rnd.randint(8, 16)):
                prod_id = len(products) + 1
                precio = round(rnd.uniform(4.99, 899.99), 2)
                products.append((prod_id, cat_id, f"{categoria} modelo {prod_id:04d}",
                                 f"Producto {prod_id} de {categoria.lower()} ({departamento})",
                                 precio, f"https://tienda.example/img/{prod_id}.jpg"))

    pesos_ciudad = [c[3] for c in CIUDADES]
    customers = []
    for cust_id in range(1, NUM_CUSTOMERS + 1):
        nombre, apellido = rnd.choice(NOMBRES), rnd.choice(APELLIDOS)
        ciudad, provincia, prefijo, _ = rnd.choices(CIUDADES, weights=pesos_ciudad)[0]
        customers.append((cust_id, nombre, apellido,
                          f"cliente{cust_id}@correo.example", "XXXXXXXXX",
                          f"{rnd.choice(CALLES)}, {rnd.randint(1, 150)}",
                          ciudad, provincia, f"{prefijo}{rnd.randint(1, 999):03d}"))

    estados, pesos_estado = zip(*ESTADOS)
    inicio = datetime(2025, 1, 1)
    orders, order_items = [], []
    for order_id in range(1, NUM_ORDERS + 1):
        fecha = inicio + timedelta(minutes=rnd.randint(0, 365 * 24 * 60))
        orders.append((order_id, fecha, rnd.randint(1, NUM_CUSTOMERS),
                       rnd.choices(estados, weights=pesos_estado)[0]))
        for producto in rnd.sample(products, rnd.randint(1, 5)):
            cantidad = rnd.randint(1, 5)
            order_items.append((len(order_items) + 1, order_id, producto[0], cantidad,
                                round(cantidad * producto[4], 2), producto[4]))

    return {
        "departments": departments,
        "categories": categories,
        "products": products,
        "customers": customers,
        "orders": orders,
        "order_items": order_items,
    }


def main():
    forzar = "--force" in sys.argv
    conexion = conectar()
    with conexion.cursor() as cursor:
        if ya_cargada(cursor) and not forzar:
            print(f"{DATABASE} ya tiene datos. Usa --force para recargarla.")
            return

        print(f"Creando las tablas de {DATABASE}")
        for sentencia in DDL:
            cursor.execute(sentencia)

        for tabla, filas in generar(random.Random(SEED)).items():
            marcadores = ", ".join(["%s"] * len(filas[0]))
            for i in range(0, len(filas), 5000):
                cursor.executemany(f"INSERT INTO {tabla} VALUES ({marcadores})", filas[i:i + 5000])
            print(f"  {tabla:<12} {len(filas):>7} filas")

    conexion.commit()
    conexion.close()
    print(f"{DATABASE} cargada")


if __name__ == "__main__":
    main()
