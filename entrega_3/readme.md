#Docker-Instrucciones
correr docker build 
 <<docker build . -t entrega_3>>

levantar container
<< docker run --rm -d -p 8080:8080 entrega_3 >>

entrar a la linea de comando del container

<< docker exec -it <id_container> /bin/bash >>

configurar usuario generico de airflow

# create an admin user
airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org


#Crear contraseña de Airflow
Username: Admin
Password: admin

correr dags en airflow


descargar archivo config.ini, crear un directorio dentro de la carpeta entrega_3 con el nombre de config y poner el archivo descargado
