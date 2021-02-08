#!/usr/bin/env node

//to visualize data, visit https://dev1.soichi.us/elk/

const express = require('express');
const config = require('./config');
const amqplib = require('amqplib');
const elasticsearch = require('@elastic/elasticsearch');

const app = express();
app.get('/health', (req, res) => {
    res.json({status: "ok"});
})
app.listen(config.express.port, () => console.log(`audit api listening!`))

console.log("connecting to elasticsearch");
const es = new elasticsearch.Client(config.elasticsearch);

console.log("connecting to amqp");
amqplib.connect(config.amqp).then(conn=>{
    conn.createChannel().then(ch=>{
        console.log("connected to amqp. now setting up audit queue");

        ch.assertQueue("audit");
	//           queue    source-ex    pattern
        ch.bindQueue("audit", "warehouse", "#");
        ch.bindQueue("audit", "amaretti", "#");
        ch.bindQueue("audit", "auth", "#");

        ch.assertExchange("audit", "topic");
        ch.assertQueue("audit-fail");
        ch.bindQueue("audit-fail", "audit", "fail.*");

        ch.consume("audit", msg=>{
            handleMessage(msg, err=>{
                if(err) {
		    console.error(JSON.stringify(err, null, 4));
		    //TODO - I don't think this works?
		    ch.publish("audit", "fail."+msg.fields.routingKey, msg.content); //publish to failed queue
		    ch.ack(msg);
		} else ch.ack(msg);
            });
        }); 
    });
});

function handleMessage(msg, cb) {
/*
{ fields:
   { consumerTag: 'amq.ctag--uzYGUm7InaitXpPfa3wQA',
     deliveryTag: 1,
     redelivered: false,
     exchange: 'auth',
     routingKey: 'user.login.1' },

*/
    let event = JSON.parse(msg.content.toString());
    let exchange = msg.fields.exchange;
    let routingKey = msg.fields.routingKey;
    /*
{ type: 'github',
  username: 'hayashis',
  exp: 1566352401.746,
  headers:
   { host: 'dev1.soichi.us',
     'x-real-ip': '45.16.200.251',
     'x-forwarded-for': '45.16.200.251',
     'x-forwarded-proto': 'https',
     connection: 'close',
     'upgrade-insecure-requests': '1',
     'user-agent':
      'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.100 Safari/537.36',
     'sec-fetch-mode': 'navigate',
     'sec-fetch-user': '?1',
     accept:
      'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,'
     'sec-fetch-site': 'cross-site',
     referer: 'https://dev1.soichi.us/auth/',
     'accept-encoding': 'gzip, deflate, br',
     'accept-language': 'en-US,en;q=0.9,ja-JP;q=0.8,ja;q=0.7' },
  timestamp: 1565747601.752 }
    */
    console.log("............", exchange, routingKey);

    let body = event;

    //parse exchange/routingKey
    let tokens = routingKey.split(".");
    let type;
    if(exchange == "auth") {
        if(routingKey.startsWith("user.login.")) {
            type = "user.login";
            body.sub = tokens[2];
        }
        if(routingKey.startsWith("user.create.")) {
            type = "user.create";
	    //body._profile = JSON.stringify(body._profile); //this might be kill the audit logging?
            body.sub = tokens[2];
        }
        if(routingKey.startsWith("user.update.")) {
            type = "user.update";
            body.sub = tokens[2];
        }
        if(routingKey.startsWith("user.refresh.")) {
            type = "user.refresh";
            body.sub = tokens[2];
        }
        if(routingKey.startsWith("user.setpass_fail.")) {
            type = "user.setpass_fail";
            body.sub = tokens[2];
        }
        if(routingKey.startsWith("group.create.")) {
            type = "group.create";
            //unpopulate admins / members to subs so that ES won't balk
            body.admins = body.admins.map(m=>m.sub||m);
            body.members = body.members.map(m=>m.sub||m);
            body.group = tokens[2];
        }
        if(routingKey.startsWith("group.update.")) {
            type = "group.update";
            //unpopulate admins / members to subs so that ES won't balk
            body.admins = body.admins.map(m=>m.sub||m);
            body.members = body.members.map(m=>m.sub||m);
            body.group = tokens[2];
        }
        if(routingKey.startsWith("user.login_fail")) {
            type = "user.login_fail";
            //body.sub = tokens[2];
        }
    }

    if(exchange == "warehouse") {
        if(routingKey.startsWith("dataset.download.")) {
            type = "dataset.download";
            body.sub = tokens[2];
            body.project = tokens[3];
            body.dataset = tokens[4];
        }
        if(routingKey.startsWith("dataset.create.")) {
            type = "dataset.create";
            body.sub = tokens[2];
            body.project = tokens[3];
            body.dataset = tokens[4];
        }
        if(routingKey.startsWith("dataset.update.")) {
            type = "dataset.update";
            body.sub = tokens[2];
            body.project = tokens[3];
            body.dataset = tokens[4];

            //slim down the event
            delete body.prov;
            delete body.product;
        }
        if(routingKey.startsWith("rule.create.")) {
            type = "rule.create";
            body.sub = tokens[2];
            body.project = tokens[3];
            body.rule = tokens[4];
        }
        if(routingKey.startsWith("rule.update.")) {
            type = "rule.update";
            body.sub = tokens[2];
            body.project = tokens[3];
            body.rule = tokens[4];
        }
        if(routingKey.startsWith("project.create.")) {
            type = "project.create";
            body.sub = tokens[2];
            body.project = tokens[3];
        }
        if(routingKey.startsWith("project.update.")) {
            type = "project.update";
            body.sub = tokens[2];
            body.project = tokens[3];
            
            //slim down the event
            delete body.stats;
        }
    }

    if(exchange == "amaretti") {
        if(routingKey.startsWith("task.create.")) {
            type = "task.create";
            body.group = tokens[2];
            body.sub = tokens[3];
            body.instance = tokens[4];
            body.task = tokens[5];
        }
        if(routingKey.startsWith("task.rerun.")) {
            type = "task.rerun";
            body.group = tokens[2];
            body.sub = tokens[3];
            body.instance = tokens[4];
            body.task = tokens[5];
        }
        if(routingKey.startsWith("task.stop.")) {
            type = "task.stop";
            body.group = tokens[2];
            body.sub = tokens[3];
            body.instance = tokens[4];
            body.task = tokens[5];
        }
        if(routingKey.startsWith("task.remove.")) {
            type = "task.remove";
            body.group = tokens[2];
            body.sub = tokens[3];
            body.instance = tokens[4];
            body.task = tokens[5];
        }
        if(routingKey.startsWith("task.ls.")) {
            type = "task.ls";
            body.group = tokens[2];
            body.sub = tokens[3];
            body.instance = tokens[4];
            body.task = tokens[5];
        }
        if(routingKey.startsWith("task.download.")) {
            type = "task.download";
            body.group = tokens[2];
            body.sub = tokens[3];
            body.instance = tokens[4];
            body.task = tokens[5];
        }
        if(routingKey.startsWith("task.upload.")) {
            type = "task.upload";
            body.group = tokens[2];
            body.sub = tokens[3];
            body.instance = tokens[4];
            body.task = tokens[5];
        }
    }

    body.timestamp = new Date(event.timestamp*1000);

    if(!type) {
        console.error("unknown exchange/routingKey.. ignoring");
        return cb();
    }

    let index = "audit."+exchange+"."+type;
    //console.log(index);
    //console.dir(JSON.stringify(body, null, 4));

    //Field [_id] is a metadata field and cannot be added inside a document.
    if(body._id) {
        body.mongo_id = body._id;
        delete body._id;
    }
    es.index({ index,body }, (err,res,status)=>{
        if(err) return cb(err);
        console.log(res.statusCode);
        cb();
    });
}


