'use strict';

console.log('loading function');

var AWS = require('aws-sdk');
var dynamo = new AWS.DynamoDB.DocumentClient();
var host = "https://5qykueb321.execute-api.us-east-1.amazonaws.com/test/";
var https = require('https');
var sns = new AWS.SNS();
var TopicArn = "arn:aws:sns:us-east-1:512959748022:sendToslack";
var dict = {
    "Property" : 1,
    "Franchise" : 2,
    "Series" : 3,
    "Episode" : 4
};
var dict1 = {
    "1" : "Property",
    "2" : "Franchise",
    "3" : "Series",
    "4" : "Episode"
};
var code ="1234567890";
var getForeignKey = "";
// var request = require('request');
var util = require('util');
var slack_url = "https://hooks.slack.com/services/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";

function send_message(message) {
    var options = {
        method: 'POST',
        hostname: 'hooks.slack.com',
        port: 443,
        path: slack_url
    };

    var postData = {
        "channel": "#sns",
        "username": "AWS SNS via Lamda :: DevQa Cloud",
        "text": "*" + message+ "*",
        "icon_emoji": ":aws:"
    };
    var req = https.request(options, function(res) {
      res.setEncoding('utf8');
      res.on('data', function (chunk) {
      });
    });

    req.on('error', function(e) {
      console.log('problem with request: ' + e.message);
    });    

    req.write(util.format("%j", postData));
    req.end();
}


function isEmpty(obj) {
    for(var prop in obj) {
        if(obj.hasOwnProperty(prop)){
            return false;
        }
    }
    return true;
}
function theCallback(err, data, callback){
    console.log("getCustomer:Before callback");
    
    if (data) {
        //callback(null, JSON.stringify(data));
        callback(null, data);
        console.log("theCallback: data = " + JSON.stringify(data));
    }
    if (err) {
        callback(err, null);
        console.log("theCallback: failure = " + JSON.stringify(err));
    }
}
exports.handler = function(event, context, callback) {

    //callback(null, event.operation); //this callback prevent the error code
    console.log(event);
    console.log("In handler");
    console.log("handler: event = " + JSON.stringify(event));
    console.log("handler: context = " + JSON.stringify(context));
    var operation = '';
    var messageFromSns = false;
    if (event.hasOwnProperty('operation')) {
        operation = event.operation;
    } else {
        var record = event.Records[0];
        var sns = record.Sns;
        console.log(sns);
        console.log(sns.Message);
        var message = JSON.parse(sns.Message);
        console.log(message);
        operation = message.operation;
        event = message;
        messageFromSns = true;
    }
    switch (operation) {
        case 'create':
            createContent(event, theCallback, callback, context, messageFromSns);
            break;
        case 'delete':
            deleteContent(event, theCallback, callback, context,messageFromSns);
            break;
        case 'read':
            findContent(event, theCallback, callback, context,messageFromSns);
            break;
        case 'update':
            updateContent(event, theCallback, callback, context,messageFromSns);
            break;
        default:
            callback(new Error('Unrecognized operation  "${event.operation}"'));
    }
};

function getTableName(key) {
    var tablename = "";
    if (key.charAt(0) == 'P') {
        tablename = "Property";
    } else if (key.charAt(0) == 'F') {
        tablename = "Franchise";
    } else if (key.charAt(0) == 'S') {
        tablename = "Series";
    } else {
        tablename = "Episode";
    }
    return tablename;
}
function findContent(event, theCallback, callback, context,messageFromSns) {
    var data = event.Key;
    var key = data.identity;
    var tablename = getTableName(key);
    var param = {
        TableName : tablename,
        Key : {
            "identity" : key
        }
    };
    dynamo.get(param, function(err, data) {
        if (err) {
            callback(err);
        } else {
            var message = tablename + " read";
            send_message(message);
            sns.publish({
                Message: tablename + 'Read',
                TopicArn: TopicArn
            }, function(err, data) {
                if (err) {
                    console.log(err.stack);
                    return;
                }

                console.log('push sent');
                console.log(data);
                // context.done(null, 'Function Finished!'); 
            });
            console.log(data);
            callback(null, data);
        }
    });
    
}
function updateContent(event, theCallback, callback, context,messageFromSns) {
    var datakey = event.Key;
    var key = datakey.identity;
    var Item = event.Item;
    var tablename = getTableName(key);
    var params = {
        TableName : tablename,
        Key: {"identity" : key},
    };
    console.log(params);
    dynamo.get(params, function(err, data){
    if (err) {
            var myErrorObj = {
                errorType : "Bad Request",
                httpStatus : 400,
                requestId : context.awsRequestId,
                message : "Undefined key."
            };
        callback(JSON.stringify(myErrorObj));
        console.log ("Undefined key");

    }else{
        if(!isEmpty(data)){
            var par = {
                TableName : tablename,
                Key: {"identity" : key},
                UpdateExpression : "SET parent = :parent, title = :title",
                ExpressionAttributeValues: {
                    ":parent" : Item.parent,
                    ":title" : Item.title
                }             
            };
            dynamo.update(par,function(err, data1) {
                    if (err) {
                        console.log('ERROR: Dynamo failed: ' + err);
                    } else {
                        var message = tablename + " updated";
                        send_message(message);
                        sns.publish({
                            Message: tablename + 'Updated',
                            TopicArn: TopicArn
                        }, function(err, data) {
                            if (err) {
                                console.log(err.stack);
                                return;
                            }

                            console.log('push sent');
                            console.log(data);
                            // context.done(null, 'Function Finished!'); 
                        });
                        // callback(null, "Success");
                        // console.log('Dynamo Success: ' + JSON.stringify(data, null, '  '));
                    }
            });
        } else {  
            var errorhere = {
                errorType : "Bad Request",
                httpStatus : 400,
                requestId : context.awsRequestId,
                message : "You are trying to update content that doesn't exist."
            };
            callback(JSON.stringify(errorhere));
            console.log("content doesn't exist!");
        }    
    }
});

}
function deleteContent(event, theCallback, callback, context,messageFromSns) {
    var Item = event.Item;
    var type = Item.type;
    var level = dict[type];
    getForeign(1,level, Item, callback, true);
    console.log(getForeignKey);
}


function deleteSelf(level, selfId) {
    var index = level.toString();        
    var tablename = dict1[index];
    var param = {
        TableName : tablename,
        Key : {
            "identity" : selfId
        }
    };
    dynamo.delete(param, function(err, data) {
       if(err) {
           console.log(err);
       } else {
            var message = tablename + " deleted";
            send_message(message);
            sns.publish({
                Message: tablename + 'Deleted',
                TopicArn: TopicArn
            }, function(err, data) {
                if (err) {
                    console.log(err.stack);
                    return;
                }

                console.log('push sent');
                console.log(data);
                // context.done(null, 'Function Finished!'); 
            });
            deleteAll(level + 1, getForeignKey);
            console.log("Delete root success!!");
       }
    });
    
}
function deleteAll(level, ParentId) {
    if (level == 5) return;
    console.log(level);
    console.log(ParentId);
    var index = level.toString();        
    var tablename = dict1[index];
    var param = {
            TableName : tablename,
            FilterExpression : "parent = :parentName",
            ExpressionAttributeValues : {
                ":parentName" : ParentId
            }
        };

    dynamo.scan(param, function(err, data) {
        if (err) {
            console.log(err);
        } else {
            if (data.Items && data.Items.length >= 1) {
                data.Items.forEach(function(item) {
                    console.log(item);
                   var deleteItem = {
                       TableName : tablename,
                       Key : {
                           "identity" : item.identity
                       }
                   };
                   dynamo.delete(deleteItem, function(err, data) {

                       if (err) {
                           console.log(err);
                       } else {
                           console.log("Success");
                       }
                   });
                   deleteAll(level + 1, item.identity);
                });
            } else {
                console.log(param);
            }
        }
    });
}
function GenerateID() {
    var rightKey = "";
    for (var i = 1; i <= 5; i++) {
        rightKey += code.charAt(Math.floor(Math.random() * 10));
    }
    return rightKey;
}
function putItem(i,level, Item,callback2) {
    var index = i.toString();        
    var tablename = dict1[index];
    var Id = tablename.charAt(0) + GenerateID();
    console.log(Id);
    var parms = {
        TableName : tablename,
        Item : {
            "identity" : Id,
            "parent" : getForeignKey,
            "title" : Item[tablename]
        }
    };
    dynamo.put(parms, function(err, data) {
        if (err) {
            console.log("Failed");
            console.log(err);
            return;
        } else {
        
            sns.publish({
                Message:'created success',
                TopicArn: 'arn:aws:sns:us-east-1:512959748022:sendToslack'
            }, function(err, data) {
                if (err) {
                    console.log(err.stack);
                    return;
                }
                console.log('push sent');
                console.log(data);
            });
            console.log("Success!!");
        }
    });
}
function getForeign(i, level, Item,callback2, isDelete) {
        if (i == level && isDelete === false) {
            putItem(i, level, Item,callback2);
            return;
        }
        var index = i.toString();        
        var tablename = dict1[index];
        console.log(tablename);
        var param = {
            TableName : tablename,
            FilterExpression : "title = :parentName",
            ExpressionAttributeValues : {
                ":parentName" : Item[tablename]
            }
        };
        console.log(param);
    dynamo.scan(param, function(err, data) {
       if(err) {
           console.log(err);
           getForeignKey = "";
           return;
       } else {
           if (data.Items && data.Items.length >= 1) {
                var output = data.Items[0];
                console.log(output);
                getForeignKey = output.identity;
                if (i == level) {
                    deleteSelf(level,getForeignKey);
                    return;
                }
                getForeign(i + 1,level,Item,callback2,isDelete);
                return;
           } else {
                var message = "Please add type : " + tablename + ": " + Item[tablename] + " before we can handle your request";
                var myErrorObj = {
                    errorType : "Bad Request",
                    httpStatus : 400,
                    message : message
                }; 
               callback2(JSON.stringify(myErrorObj));
               getForeignKey = "";
               return;               
           }
       }
    });
}


function createContent(event, callback1, callback2, context,messageFromSns){
    var Item = event.Item;
    var type = Item.type;
    var tablename = "";
    var rightKey = GenerateID();
    console.log(typeof rightKey);
    if (type == "Property") {
        var key = "P" + rightKey;
        console.log("property is " + key);
        var parms = {
            TableName : "Property",
            Item : {
                "identity" : key,
                "title" : Item.Property
            }
        };
        dynamo.put(parms, function(err, data) {
            if (err) {
                var myErrorObj = {
                    errorType : "Bad Request",
                    httpStatus : 400,
                    requestId : context.awsRequestId,
                    message : "Invalid Input."
                };
                if (messageFromSns === false) {
                    callback2(JSON.stringify(myErrorObj));
                } else {
                    sns.publish({
                        Message: 'Fail to Create' + type,
                        TopicArn: TopicArn
                    }, function(err, data) {
                        if (err) {

                            console.log(err.stack);
                            return;
                        }
                        console.log('push sent');
                        console.log(data);
                        context.done(null, 'Function Finished!');  
                    });
                }
            } else {
                var message = type + " created";
                send_message(message);
                sns.publish({
                    Message: type + 'Created',
                    TopicArn: TopicArn
                }, function(err, data) {
                    if (err) {
                        console.log(err.stack);
                        return;
                    }

                    console.log('push sent');
                    console.log(data);
                    // context.done(null, 'Function Finished!'); 
                });
                                 
            }
        });
        return;
    }
    var level = dict[type];
    console.log(level);
    getForeign(1,level,Item,callback2, false);
}