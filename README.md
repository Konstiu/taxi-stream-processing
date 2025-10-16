run with 

~~~ 
docker compose down --remove-orphans
docker compose up --build -d
docker compose logs -f feeder
~~~



There has to be a folder called data in the same directory as the docker-compose.yml file. In that folder there have to be the csv files with the data to be imported.

<img width="592" height="560" alt="image" src="https://github.com/user-attachments/assets/82ad9f74-6488-4a59-be63-7b3fe7da3d00" />
