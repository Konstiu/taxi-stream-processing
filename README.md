run with 

~~~ 
docker compose down --remove-orphans
docker compose up --build -d
docker compose logs -f feeder
~~~



There has to be a folder called data in the same directory as the docker-compose.yml file. In that folder there have to be the csv files with the data to be imported.

