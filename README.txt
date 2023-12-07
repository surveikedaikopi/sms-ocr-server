sudo docker build -t webhook .
sudo docker run -d -p 8008:8008 --env-file .env webhook

sudo docker logs <containerID>
sudo docker exec -it <containerID> /bin/bash


sudo docke image prune -a
sudo docker container prune
sudo docker volume prune
sudo docker network prune
df -h


http://4.194.114.42:8008/docs
http://4.194.114.42:8008/receive
http://4.194.114.42:8008/read

ssh -i ~/Downloads/dev-iot_key.pem PipelineSecret@4.194.114.42
