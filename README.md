# TRABAJO PRÁCTICO 2: Base de datos no SQL Replicada (en memoria)
Este repositorio tiene los archivos de código en lenguaje Erlang para resolver las consignas del trabajo práctico 2 de la materia Programación Concurrente.
## INTEGRANTES:
- Lazzarini Bautista				bautylazza@gmail.com
- Nicle Santiago				niclesantiago@gmail.com
- Ramella Sebastián				ramellasebastian02@gmail.com

El trabajo se divide en dos partes. En primer lugar, se presenta la implementación de una base de datos no replicada, que funciona como un proceso único encargado de almacenar pares clave-valor en memoria, gestionando operaciones de inserción, eliminación y consulta con control de versiones mediante timestamps. En esta versión, se establece una estructura simple de servidor que interactúa con los clientes a través del paradigma de paso de mensajes, garantizando concurrencia y eficiencia en el procesamiento de solicitudes.  
En la segunda parte del trabajo, se desarrolla la base de datos replicada, que extiende el modelo anterior incorporando múltiples réplicas que trabajan en conjunto para almacenar y gestionar los datos. En esta versión, las operaciones pueden ejecutarse en cualquier réplica, que a su vez actúa como coordinadora para distribuir la solicitud entre el resto de las réplicas. Para asegurar la consistencia, el sistema permite definir tres niveles de consistencia:  
- one: La operación se confirma tras ejecutarse en una sola réplica.  
-	quorum: Se espera la confirmación de al menos la mitad de las réplicas antes de dar una respuesta.  
-	all: La operación solo se confirma cuando todas las réplicas han procesado la solicitud.

El desarrollo de este sistema en Erlang aprovecha su modelo basado en actores y comunicación por mensajes para gestionar múltiples procesos concurrentes sin bloqueos. 
