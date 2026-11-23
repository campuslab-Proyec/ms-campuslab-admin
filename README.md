# ms-campuslab-admin

Microservicio administrador de **Kafka** y **RabbitMQ** de CampusLab (puerto 8086).
Todas las rutas exigen un JWT válido de Entra ID (`Authorization: Bearer <token>`).

## Kafka — `/api/admin/kafka`
| Método | Ruta | Qué hace |
|---|---|---|
| GET | `/topics` | Lista tópicos |
| GET | `/topics/{name}` | Particiones (líder, réplicas, ISR) y configuración (retención, cleanup.policy, etc.) |
| POST | `/topics` | Crea un tópico `{name, partitions, replicationFactor, configs}` |
| PUT | `/topics/{name}/config` | Modifica configuración `{configs:{...}}` |
| PUT | `/topics/{name}/partitions` | Aumenta particiones `{total}` |
| DELETE | `/topics/{name}` | Elimina un tópico |
| GET | `/consumer-groups` | Lista grupos |
| GET | `/consumer-groups/{id}` | Estado, miembros, offsets y lag |
| POST | `/consumer-groups/{id}/reset-offsets?topic=` | Reinicia el grupo al inicio |
| DELETE | `/consumer-groups/{id}` | Elimina el grupo |

## RabbitMQ — `/api/admin/rabbit`
| Método | Ruta | Qué hace |
|---|---|---|
| GET / POST | `/queues` | Lista / crea cola `{name, durable, arguments}` (DLQ con `x-dead-letter-exchange`) |
| DELETE | `/queues/{name}` | Elimina cola |
| GET / POST | `/exchanges` | Lista / crea exchange `{name, type, durable}` |
| DELETE | `/exchanges/{name}` | Elimina exchange |
| GET / POST | `/bindings` | Lista / crea binding `{source, destination, destinationType, routingKey}` |
| POST | `/bindings/delete` | Elimina binding (mismo cuerpo) |

Variables: `SPRING_KAFKA_BOOTSTRAP_SERVERS`, `SPRING_RABBITMQ_HOST`, `RABBITMQ_MANAGEMENT_URL`, `AZURE_TENANT_ID`.
