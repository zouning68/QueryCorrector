docker rm -f query_correct
docker kill query_correct
docker build -t ic/querycorrect:v$1 --no-cache .
docker run -d --name query_correct -v /opt/userhome/algo/querycorrectlog:/server/log -p 51668:51668 --net='host' ic/querycorrect:v$1
#docker run -it --name query_correct -v /opt/userhome/algo/querycorrectlog:/server/log -p 51668:51668 --net='host' ic/querycorrect:v$1 bash


#docker run -it --name query_correct -v /opt/userhome/algo/querycorrectlog:/server/log -p 51668:51668 --net='host' ic/querycorrect:v$1 bash
#docker exec -it query_correct
