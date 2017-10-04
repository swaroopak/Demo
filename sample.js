var express = require('express')
var app = express()
var http = require('http')
var request = require('request');
sys = require('sys');
var async = require("async");
var asyncLoop = require('node-async-loop');
var config = require('./config');
var _ = require('lodash');


function getUrlOfServer(reqCount)
{
    return config.server[reqCount%config.server.length];
}

//simple novice approch to determine the where to route the request based on health and memory and cpu usage
function nextRoute(cb, failedServers){
    var server = _.difference(config.server, failedServers);
    if(server.length==0){
        cb("no server available");
        return;
    }
    var number = 0;
    
    //take healthyServer List
    var usage = [];

    async.waterfall([
    	function(cb){
    		async.map(server, function(url, next){
		    	var urlNew = "http://" + url +"/health";
		        request(urlNew, function (error, response, body) {            
		            var response = JSON.parse(body);                        
		            if(response.status_code == 200){
		            	healthyServer.push(url);
		            }
		            next();
		        });
    		}, function(err, reults){
		        cb(err)
		    })
    	},
    	function(cb){
		    async.map(healthyServer, function(url, next){
		        url = "http://" + url +"/status";		        
		        request(url, function (error, response, body) {  		        	          
		            var response = JSON.parse(body);                        
		            usage.push(response.cpu_usage * 70 + response.memory_usage * 30);
		            next();
		        });
		    }, function(err, results) {    	
		        var minUsage = Number.MAX_VALUE;
			    for(var i = 0; i < healthyServer.length; i++){
			    	if(minUsage > healthyServer[i]){
			    		number = i;
			    		minUsage = healthyServer[i];
			    	}
			    }	    	    
			    console.log("The Usage is: ");
			    console.log(usage);
		        cb(err, healthyServer[number]);        
		    })
		}
	    ], function(err, server){
	    	if(err) console.log(err);
	    	cb(err, server);
	    });
    
}



//swaroopa old writeup
callback = function(response) {
    var str = '';

    //another chunk of data has been recieved, so append it to `str`
    response.on('data', function (chunk) {
        var response = JSON.parse(chunk);
        str = response.status_code;
        console.log("The response after routing: "+ str);
    });

    //the whole response has been recieved, so we just print it out here
    // response.on('end', function () {
   //   console.log(str);
    // });
}

var reqCount=0;

app.get('/query', function (req, res) {
    var failedServers = [];
    async.retry(3, function(cb){
        async.waterfall([
            function(cb){
                reqCount+=1;
                nextRoute(cb, failedServers);
            },
            function(url, cb){
                console.log("redirecting query to "+ url);
    	        var urlNew = "http://" + url +"/query";
    	        console.log(url);//should print out url;        
    	        request(urlNew, {timeout: 1000}, function (error, response, body) {
                    if(error){
                        console.log("req timed out");
                        failedServers.push(url);
                    }          
                    cb(error, body);
                });
            }
        ], function(err, result){
            cb(err, result);
        });
    }, function(err, result){
        if(err){
            res.send("error");
        }else{
            res.send(result);
        }
    });

})


var requests = {};

var reqId = 0;


function processRequest(reqObject, failedServers, cb){
        async.waterfall([
            function(cb){
                reqCount+=1;
                nextRoute(cb, failedServers);
            },
            function(url, cb){
                console.log("redirecting request to "+ url);
                var urlNew = "http://" + url +"/req";
                console.log(url);//should print out url;    

                var options ={
                    url:urlNew,
                    json: true,
                    body: {
                        req_id:reqObject.req_id,
                        duration: reqObject.duration
                    },
                    method: "post"
                }

                request(options, function (error, response, body) {
                    if(!error){
                        reqObject.instance_url=url
                        requests[reqObject.req_id] = reqObject;
                    }
                    cb(error, body);
                });
            }
        ], function(err, result){
            cb(err, result);
        });
}
app.get('/req', function (req, res) {
    var failedServers = [];
            var start_time = new Date().valueOf();
            var duration = (req.query.duration) ? req.query.duration : 1;
                var reqObject= {
                    req_id: reqId++,
                    duration: duration,//in minutes
                    start_time: start_time,
                    end_time:start_time + duration*60*1000,
                    next_check: start_time+ duration*60*1000/5
                }       
        async.waterfall([
            function(cb){
                processRequest(reqObject, [], cb);
            }
        ], function(err, result){
            if(err){
                res.send(err);
            }else{
                res.send("request");
            }
        });
})



app.listen(config.port, function () {
      console.log('Example app listening on port 3000!')
})

setInterval(function(){
    //for each request object in requests, check
    var currTime=new Date().valueOf();

    Object.keys(requests).forEach(function(key){
    
        if(requests[key].next_check<=currTime){
                var url=requests[key].instance_url
                var urlNew = "http://" + url +"/reqStatus?id="+key;
                request(urlNew, function (error, response, body) {
                    if(error){
                        console.log("req timed out");
                    }       
                    if(response.body=='1'){
                        requests[key].next_check=requests[key].next_check+requests[key].duration*60*1000/5
                    }
                    else{
                        console.log("relocating dead thing");
                        async.waterfall([
                            function(cb){
                                processRequest(requests[key], [url], cb);
                            }
                        ], function(err, result){
                            console.log("mark");
                            if(err){
                                console.log(err);
                            }
                        });
                    }
                });            
        }
    })
}, 5000);
