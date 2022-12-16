**Installations required:**

- Install Java-17
- Install docker compose using this.
- Install Postman to access the application via REST APIs

**System Setup :**

- Create Spring boot applications Jar (mvn clean package) 
- Docker setup having 4 Containers  in a same network (Bridge) having its own static IP (docker-compose up)
- Once docker containers are up and running each Node can be accessed from host machine on Ports: N1-8080,N2-8081,N3-8082,N4-8083
- Each container hosts spring application (8080 port) 
- Once containers are up and running we can access the application running on any container from the host machine via Postman.
- We can access all containers via - http://IP_of_Host_Machine:Port_Number (where Port_Number is different for each container).<br />Eg: http://172.17.87.180:8082/healthCheck

**APIs :**

- /object/store : To store the file in the key value store
- /object/retrieve/{filename} : To retrieve the file from key value store with vector clocks
- /healthCheck: To check if container is running fine

<ins>NOTE:</ins><br />You can find our design document here: https://docs.google.com/document/d/1GZMHRwbuv1zDORnIzKQigy6dzFVnOLyQeVQHX90OneQ/edit?usp=sharing
