run with 

~~~ 
docker compose down --remove-orphans
docker compose up --build -d
docker compose logs -f feeder
~~~



There has to be a folder called data in the same directory as the docker-compose.yml file. In that folder there have to be the csv files with the data to be imported.

<img width="592" height="560" alt="image" src="https://github.com/user-attachments/assets/82ad9f74-6488-4a59-be63-7b3fe7da3d00" />




Kafka can be viewed from the Internet with the URL http://localhost:8085/

<img width="3100" height="1116" alt="image" src="https://github.com/user-attachments/assets/0875728f-12fe-4747-9fb9-39d960da501a" />





On storm, you can view live data updates with the command: 
~~~
docker exec -it redis redis-cli hgetall taxi:100:state
docker exec -it redis redis-cli lrange taxi:100:track 0 5
~~~
this looks at the live data of taxi with id 100
<img width="944" height="501" alt="image" src="https://github.com/user-attachments/assets/010e0876-019d-42bf-82d1-7b111f6ac28f" />
