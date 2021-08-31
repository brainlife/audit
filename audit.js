#!/usr/bin/env node

//to visualize data, visit https://dev1.soichi.us/elk/

const express = require('express');
const config = require('./config');
const amqp = require('amqp-connection-manager');
const elasticsearch = require('@elastic/elasticsearch');

console.log("starging audit service");

//start health api..
const app = express();
app.get('/health', (req, res) => {
    res.json({status: "ok"});
})
const server = app.listen(config.express.port, () => console.log(`audit api listening!`))

console.log("connecting to elasticsearch");
const es = new elasticsearch.Client(config.elasticsearch);

/* makes it impossible to terminate
setInterval(()=>{
    es.info(console.log);
}, 1000*60);
*/

console.log("connecting to amqp");
const conn = amqp.connect([config.amqp]);
const ch = conn.createChannel({
    json: true,
    setup: (ch)=>{
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
    },
});

console.log("consuming..");

//TODO - elasticsearch connection piles up for some reason.. let's restart every once a while
setTimeout(()=>{
    console.log("timeout.. closing to clen up elasticsearch connections");
    conn.close();
    es.close();
    server.close();
}, 1000*600);

function handleMessage(msg, cb) {
    let event = JSON.parse(msg.content.toString());
    let exchange = msg.fields.exchange;
    let routingKey = msg.fields.routingKey;

    //we can't just whatever in the mongo collection to elasticsearch 
    //it will lead to "java.lang.IllegalArgumentException: Limit of total fields [1000] in index [audit.warehouse.rule.update] has been exceeded"
    //let body = event; 

    let body = {}; //start out empty, and only add things that we really need indexed in the elasticsearch

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
            body.admins = event.admins.map(m=>m.sub||m);
            body.members = event.members.map(m=>m.sub||m);
            body.group = tokens[2];
        }
        if(routingKey.startsWith("group.update.")) {
            type = "group.update";
            //unpopulate admins / members to subs so that ES won't balk
            body.admins = event.admins.map(m=>m.sub||m);
            body.members = event.members.map(m=>m.sub||m);
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
        }
        if(routingKey.startsWith("datalad.import.")) {
            type = "datalad.import";
            body.sub = tokens[2];
            body.project = tokens[3];
            body.dldataset = tokens[4];
        }
        if(routingKey.startsWith("secondary.download.")) {
            type = "secondary.download";
            body.sub = tokens[2];
            body.group = tokens[3]; 
            body.task = tokens[4];
        }
    }

    if(exchange == "amaretti") {
        if(routingKey.startsWith("task.create.")) {
            type = "task.create";
            body.group = tokens[2];
            body.sub = tokens[3];
            body.instance = tokens[4];
            body.task = tokens[5];

            body.service = event.service;
            body.service_branch = event.service_branch;
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

            body.fullpath = event.fullpath;
            body.resource_id = event.resource_id;
            body.resource_name = event.resource_name;
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

            body.path = event.path;
        }
    }

    body.timestamp = new Date(event.timestamp*1000);

    console.log(exchange, routingKey, type);
    console.dir(event);

    if(!type) {
        console.error("unknown exchange/routingKey.. ignoring");
        return cb();
    }

    let index = "audit."+exchange+"."+type;

    //Field [_id] is a metadata field and cannot be added inside a document.
    if(body._id) {
        body.mongo_id = body._id;
        delete body._id;
    }
    es.index({ index,body }, (err,res)=>{
        if(err) return cb(err);
        console.log(res.statusCode, index);
        cb();
    });
}

