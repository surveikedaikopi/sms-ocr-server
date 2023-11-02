sudo docker build -t webhook .
sudo docker run -d -p 8008:8008 webhook


http://4.194.114.42:8008/docs