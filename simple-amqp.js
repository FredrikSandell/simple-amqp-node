var amqplib = require('amqplib/callback_api');
var uuid = require('uuid');
var domain = require('domain');

exports.createEndpointFactory = function (options, externalCallback) {
    function isInteger(n) {
        return Number(n) === n && n % 1 === 0;
    }

    if (options.host == undefined || options.host == null) {
        externalCallback(new Error('Option object must contain a "host" field'),null);
        return;
    }
    if (options.globalTtl == undefined || options.globalTtl == null || !isInteger(options.globalTtl)) {
        options.globalTtl = 60000; //global ttl for messages are defaulted to 60 seconds
    }
    var connection = null;
    var dom = domain.create();
    dom.on('error', function() {
        console.log('Connection failed... will attempt reconnect soon');
        if(connection != null) {
            connection = null;
            retryConnection();
        }
    });
    var BACKOFF_BASE = 300; //ms
    var BACKOFF_MAX = 10000;
    var BACKOFF_INC = 1000;
    var backoff = BACKOFF_BASE;
    function retryConnection() {
        if(connection == null) {
            setTimeout(function () {
                console.log('now attempting reconnect ...');
                amqplib.connect(options.host,function(err,conn2) {
                    if(err) {
                        console.log("reconnect failed!");
                        console.log(err);
                        if((backoff+BACKOFF_INC)<BACKOFF_MAX) {
                            backoff+=BACKOFF_INC;
                        }
                        retryConnection();
                    } else {
                        console.log('connection reestablished');
                        connection = conn2;
                        backoff = BACKOFF_BASE;
                    }
                });
            }, backoff);
        }
    }

    amqplib.connect(options.host,function(err,conn) {
        if(err) {
            externalCallback(err,null);
            return;
        }

        connection = conn;
        dom.add(connection);

        connection.on('close', function connectionClose(err) {
            console.log(err);
            externalCallback(new Error('connection closed'),null);
        });
        
        connection.on('error', function () {
            retryConnection();
        });

        function createEventReceiver(endpoint, isBroadcast, consumer) {
            var channel = null;
            console.log("creating event receiver");
            connection.createChannel(function(err, ch){
                ch.on('error', function (err) {
                    console.log("channel error");
                    consumer(err,null);
                });

                channel = ch;
                if(err) {
                    console.log("error");
                    consumer(err,null);
                }
                ch.assertExchange(endpoint, 'fanout', {
                    durable: false,
                    autoDelete: true
                }, function(err, ok) {
                    console.log(ok);
                    if(err) {
                        consumer(err,null);
                    }
                    ch.assertQueue(isBroadcast ? '' : endpoint, { //empty string gives the queue a random name
                        exclusive: isBroadcast,
                        durable: false,
                        autoDelete: false
                    }, function(err, ok) {
                        console.log("ok");
                        console.log(ok);
                        if(err) {
                            consumer(err,null);
                        }
                        //no routing pattern for queue. Everything is forwarded
                        ch.bindQueue(ok.queue, endpoint, '', {},function(err,ok) {
                            ch.consume(ok.queue, handleMessage, {noAck: true});
                        });
                    })
                })
            });
            function handleMessage(msg) {
                console.log(msg.content);
                consumer(null, msg.content.toString());
            };
            return {
                close: function () {
                    if(channel) {
                        try {
                            channel.close();
                        } catch (err) {
                            //channel is already closed
                        }
                    }
                }
            };
        }
        externalCallback(null,{

            getQueuedEventReceiver: function (endpoint, consumer) {
                return createEventReceiver(endpoint, false, consumer);
            },

            getBroadcastEventReceiver: function (endpoint, consumer) {
                return createEventReceiver(endpoint, true, consumer);
            },

            getRpcServer: function (endpoint, func) {
                var channel = null;
                connection.createChannel(function(err,ch) {
                    ch.on('error', function (err) {
                        func(err,null);
                    });
                    if(err) {
                        func(err,null);
                    }
                    channel = ch;
                    ch.assertExchange(endpoint, 'fanout', {
                        durable: false,
                        autoDelete: true
                    }, function(err,ok) {
                        if(err) {
                            func(err,null);
                        }
                        ch.assertQueue(endpoint + "_rpcQueue", {
                            exclusive: false,
                            durable: false,
                            autoDelete: true
                        },function(err,queueok) {
                            if(err) {
                                func(err);
                            }
                            //no routing pattern for queue. Everything is forwarded
                            ch.bindQueue(queueok.queue, endpoint, '',{}, function(err, queueBound){
                                if(err) {
                                    func(err,null);
                                }
                                ch.prefetch(1);
                                ch.consume(queueBound.queue, handleMessage, {noAck: true});
                            });
                        })
                    })
                });
                function handleMessage(msg) {
                    var response = func(null,msg.content.toString());
                    channel.sendToQueue(msg.properties.replyTo,
                        new Buffer(response.toString()),
                        {correlationId: msg.properties.correlationId},function(err,ok) {
                            if(err) {
                                func(err,null);
                            }
                        });
                }
                return {
                    close: function () {
                        if(channel) {
                            try {
                                channel.close();
                            } catch (err) {
                                //channel is already closed
                            }
                        }
                    }
                };
            },

            getEventEmitter: function () {
                return {
                    emit: function (endpoint, message, callback) {
                        connection.createChannel(function(err,ch) {
                            ch.on('error', function (err) {
                                callback(err,null);
                            });

                            var dom = domain.create();
                            dom.on('error', function (err) {
                                callback(err, null);
                            });
                            dom.run(function () {
                                ch.publish(endpoint, '', new Buffer(message), {
                                    expiration: options.globalTtl,
                                    persistent: false
                                }, function(err,ok) {
                                    if(ch) {
                                        try {
                                            ch.close();
                                        } catch (err) {
                                            //channel is already closed
                                        }
                                    }
                                });
                                callback(null,true);
                            });
                        });
                    }
                };
            },

            getRpcClient: function () {

                return {
                    call: function (endpoint, message, timeout, callback) {
                        var channel = null;
                        var timeoutCallback = null;
                        var dom = domain.create();
                        dom.on('error', function (err) {
                            callback(err,null);
                        });

                        dom.run(function () {
                            if (timeout != undefined && timeout != null && isInteger(timeout) && timeout > options.globalTtl) {
                                callback(new Error('The timeout must be lower than the configured globalTtl. Currently set to ' + options.globalTtl),null);
                            }
                            connection.createChannel(function(err,ch) {
                                ch.on('error', function (err) {
                                    callback(err,null);
                                });

                                if(err) {
                                    callback(err,null);
                                }
                                channel = ch;
                                var corrId = uuid();

                                function maybeAnswer(msg) {
                                    if (msg.properties.correlationId === corrId) {
                                        clearTimeout(timeoutCallback);
                                        callback(null, msg.content.toString());
                                        if(channel) {
                                            try {
                                                channel.close();
                                            } catch(err) {
                                                //channel closed
                                            }
                                        }
                                    }
                                }
                                ch.assertQueue('', {exclusive: true}, function(err,qok) {
                                    if(err) {
                                        callback(err,null);
                                    }
                                    ch.consume(qok.queue, maybeAnswer, {noAck: true}, function(err, ok) {
                                        if(err) {
                                            callback(err,null);
                                        }
                                        ch.publish(endpoint, '', new Buffer(message.toString()), {
                                            expiration: options.globalTtl,
                                            correlationId: corrId,
                                            replyTo: qok.queue,
                                            persistent: false
                                        }, function(err, ok) {
                                            if(err) {
                                                callback(err,null);
                                            }
                                            timeoutCallback = setTimeout(function(){
                                                callback(new Error("Call timeout after "+timeout+" ms."),null);
                                                if(channel) {
                                                    try {
                                                        channel.close();
                                                    } catch(err) {
                                                        //channel closed
                                                    }
                                                }
                                            }, timeout);
                                        })
                                    })
                                })
                            })
                        });
                    }
                };
            },

            close: function () {
                return connection.close();
            }
        });
    });
};