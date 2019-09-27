# synerex_nodeserv
Synerex Node Server

# build and run with Docker
``` shell
docker build ./ -t synerex_nodesrv

# build and run nodeserv
docker run --detach --tty  --name synerex_nodesrv --rm -v $PWD:/go/src/github.com/synerex_nodesrv synerex_nodesrv

# exec bahs in docker container
docker exec -it synerex_nodesrv bash

```
