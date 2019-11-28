echo $1
docker tag ic/querycorrect:v$1 hub.ifchange.com/ic/querycorrect:v$1
docker push hub.ifchange.com/ic/querycorrect:v$1
