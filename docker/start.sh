#!/bin/bash

#This script is used inside the docker container to start api and ui(via http-server)

pm2 start /app/audit.js

pm2 logs
