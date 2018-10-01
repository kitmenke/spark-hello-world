# docker build -t solr-banana .
# docker run --name my_solr -d -p 8983:8983 -t solr-banana
# docker exec -it --user=solr my_solr bin/solr create_core -c gettingstarted
FROM solr:latest
ADD docker-install-banana.sh /docker-entrypoint-initdb.d/