
#to run the stack locally

pm2 delete audit
pm2 start audit.js --name audit --watch --ignore-watch="*.log test *.sh ui bin example .git"

pm2 save
