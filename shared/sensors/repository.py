import json

from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime

from shared.cassandra_client import CassandraClient
from shared.mongodb_client import MongoDBClient
from shared.redis_client import RedisClient
from shared.sensors import models, schemas
from shared.timescale import Timescale
from shared.elasticsearch_client import ElasticsearchClient


class DataCommand():
    def __init__(self, from_time, to_time, bucket):
        if not from_time or not to_time:
            raise ValueError("from_time and to_time must be provided")
        if not bucket:
            bucket = 'day'
        self.from_time = from_time
        self.to_time = to_time
        self.bucket = bucket


def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb_client: MongoDBClient,es: ElasticsearchClient, cs: CassandraClient) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    db_sensor_data = sensor.dict()
    mongodb_client.insertOne(db_sensor_data)
    es_doc = {
        'name': sensor.name,
        'description': sensor.description,
        'type': sensor.type
    }
    es.index_document('sensors_index', es_doc)
    
    sensor_dict = sensor.dict()
    sensor_dict['id'] = db_sensor.id
    
    ##cs.execute(f"""
    ##        INSERT INTO sensor.quantity
    ###        (sensor_id, type)
    ###        VALUES ({db_sensor.id}, {sensor.type}))
    ###        """

    
    return sensor_dict

def record_data(redis: Session, sensor_id: int, data: schemas.SensorData,timescale: Timescale) -> schemas.Sensor:
    timescale_query = f"""
                INSERT INTO sensor_data 
                (sensor_id, temperature, humidity, velocity, battery_level, last_seen) 
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (sensor_id, last_seen) DO UPDATE SET
                velocity = EXCLUDED.velocity,
                temperature = EXCLUDED.temperature,
                humidity = EXCLUDED.humidity,
                battery_level = EXCLUDED.battery_level;
                """
    values = (sensor_id, data.temperature, data.humidity, data.velocity, data.battery_level, data.last_seen)
    redis.set(sensor_id, json.dumps(data.dict(exclude_none=True)))
    timescale.insert(timescale_query, values)
    return redis.get(sensor_id)

def get_data(redis: Session, sensor_id: int) -> schemas.Sensor:
    db_data = redis.get(sensor_id)
    if db_data is None:
        raise HTTPException(status_code=404, detail="Sensor data not found")
    return schemas.SensorData(**json.loads(db_data))

def get_data_timescale(sensor_id: int, from_: str, to: str, bucket: str, timescale: Timescale) -> schemas.Sensor:
    ts_query = f"""
        SELECT time_bucket('1 {bucket}', last_seen) AS bucket_interval, AVG(velocity) AS avg_velocity, AVG(temperature) AS avg_temperature, AVG(humidity) AS avg_humidity, AVG(battery_level) AS avg_battery_level
        FROM sensor_data
        WHERE sensor_id = {sensor_id}
            AND last_seen BETWEEN '{from_}'::timestamp AND '{to}'::timestamp
        GROUP BY bucket_interval
        ORDER BY bucket_interval ASC;
    """

    timescale.execute(ts_query)
    result = timescale.getCursor().fetchall()

    return result

def delete_sensor(db: Session, sensor_id: int, mongodb_client: MongoDBClient, redis_client = RedisClient,es=ElasticsearchClient, timescale = Timescale):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    
    mongodb_client.deleteOne(db_sensor.name)
    redis_client.delete(sensor_id)
    timescale.delete_sensor('sensor_data', sensor_id)
    return db_sensor

def get_sensors_near(latitude: float, longitude: float, radius: int, db: Session, mongodb_client: MongoDBClient, redis_client = RedisClient):
    lat_min, lat_max = latitude - radius, latitude + radius
    long_min, long_max = longitude - radius, longitude + radius
    query = {
        "latitude": {"$gte": lat_min, "$lte": lat_max},
        "longitude": {"$gte": long_min, "$lte": long_max}
    }
    sensors = mongodb_client.collection.find(query)
    
    sensors_nearby = []
    for sensor in sensors:
        db_sensor = get_sensor_by_name(db, sensor['name'])
        if db_sensor:
            sensor_data = get_data(redis_client, db_sensor.id).dict()
            sensor_data["id"] = db_sensor.id
            sensor_data["name"] = db_sensor.name
            sensors_nearby.append(sensor_data)
        

    return sensors_nearby

def search_sensors(db: Session, mongodb: MongoDBClient, es: ElasticsearchClient, query: str, size: int = 10, search_type: str = "match", redis_client=RedisClient):
    query_dict = json.loads(query)

    # Define el campo de búsqueda y el valor correspondiente en el cuerpo de la consulta de Elasticsearch
    search_field = ""
    search_value = ""
    if 'type' in query_dict:
        search_field = "type"
        search_value = query_dict['type']
    elif 'name' in query_dict:
        search_field = "name"
        search_value = query_dict['name'].lower()  # Convertimos a minúsculas
    elif 'description' in query_dict:
        search_field = "description"
        search_value = query_dict['description'].lower()  # Convertimos a minúsculas
    else:
        # Si no se proporciona un campo de búsqueda válido en el query_dict, regresa una lista vacía
        return []

    # Construye el cuerpo de la consulta de Elasticsearch basado en el campo de búsqueda y el tipo de búsqueda
    es_query_body = {}
    if search_type == "match":
        es_query_body = {
            "query": {
                "match": {
                    search_field: search_value
                }
            },
            "size": size
        }
    elif search_type == "similar":
        es_query_body = {
            "query": {
                "match": {
                    search_field: {
                        "query": search_value,
                        "fuzziness": "AUTO",
                        "operator": "and"
                    }
                }
            },
            "size": size
        }
    elif search_type == "prefix":
        es_query_body = {
            "query": {
                "prefix": {
                    search_field: search_value
                }
            },
            "size": size
        }

    # Ejecuta la consulta de búsqueda
    es_response = es.search('sensors_index', es_query_body)
    sensor_names = []

    # Extrae los nombres de los sensores de la respuesta de Elasticsearch
    for hit in es_response['hits']['hits']:
        sensor_names.append(hit['_source']['name'])
    
    # Obtiene los detalles de los sensores de MongoDB utilizando los nombres de los sensores
    sensors = []
    for sensor_name in sensor_names:
        sensor_doc = mongodb.getCollection('Sensors').find_one({"name": sensor_name}, {'_id': 0})
        sensor_dict = get_sensor_by_name(db, sensor_doc["name"]).id
        db_sensor = get_sensor(db, sensor_dict)
        if db_sensor:
            sensor = get_sensor_mongo(mongodb, db_sensor)
            sensors.append(sensor)

    # Ordena los parámetros del diccionario de cada sensor según el orden deseado
    sensors_reordered = []
    for sensor in sensors:
        sensor_reordered = {
            "id": sensor["id"],
            "name": sensor["name"],
            "latitude": sensor["latitude"],
            "longitude": sensor["longitude"],
            "type": sensor["type"],
            "mac_address": sensor["mac_address"],
            "manufacturer": sensor["manufacturer"],
            "model": sensor["model"],
            "serie_number": sensor["serie_number"],
            "firmware_version": sensor["firmware_version"],
            "description": sensor["description"]
        }
        sensors_reordered.append(sensor_reordered)

    # Elimina los duplicados
    unique_sensors = [dict(t) for t in {tuple(d.items()) for d in sensors_reordered}]
    return unique_sensors

def get_sensor_mongo(mongodb: MongoDBClient, db_sensor: models.Sensor) -> schemas.SensorCreate:
    sensor_dict = mongodb.getCollection('Sensors').find_one({'name': db_sensor.name}, {'_id': 0})
    sensor_dict.update({'id': db_sensor.id})
    return sensor_dict        

def get_temperature_values(db=Session, cassandra=CassandraClient):
    return cassandra.get_values_temperature()

def get_sensors_quantity(db=Session, cassandra=CassandraClient):
    return cassandra.get_values_temperature

def get_low_battery_sensors(db=Session, cassandra=CassandraClient):
    return cassandra.get_low_battery
