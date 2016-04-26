var amqplib = require('amqplib');
var uuid = require('node-uuid');
var when = require('when');
var domain = require('domain');
var defer = when.defer;

exports.createEndpointFactory = function (options) {

    function isInteger(n) {
        return Number(n) === n && n % 1 === 0;
    }

    if (options.host == undefined || options.host == null) {
        return when.reject(Error('Option object must contain a "host" field'));
    }
    if (options.globalTtl == undefined || options.globalTtl == null || !isInteger(options.globalTtl)) {
        options.globalTtl = 60000; //global ttl for messages are defaulted to 60 seconds
    }

    return amqplib.connect('amqp://' + options.host).then(function (conn) {

        function retryConnection() {
            setTimeout(function () {
                console.log('now attempting reconnect ...');
                amqplib.connect('amqp://' + options.host).then(function (conn2) {
                    console.log('connection reestablished')
                    conn = conn2;
                }).catch(function (err) {
                    retryConnection();
                });
            }, 3000);

        }

        conn.on('close', function connectionClose() {
            console.log('')
            //retryConnection()
        });
        conn.on('error', function () {
            retryConnection()
        });

        function createEventReceiver(endpoint, consumer, isBroadcast) {
            return when(conn.createChannel().then(function (ch) {
                ch.assertExchange(endpoint, 'fanout', {
                    durable: false,
                    autoDelete: true
                }).then(function () {
                    //exchange exists
                    return ch.assertQueue(isBroadcast ? '' : endpoint, { //empty string gives the queue a random name
                        exclusive: isBroadcast,
                        durable: false,
                        autoDelete: true
                    });
                }).then(function (queueok) {
                    return ch.bindQueue(queueok.queue, endpoint, '');//no routing pattern. Everything is forwarded
                }).then(function (queueBound) {
                    return ch.consume(queueBound.queue, handleMessage, {noAck: true});
                }).then(function () {
                    return true;
                }).catch(function (error) {
                    console.log(error);
                    return false;
                });
                function handleMessage(msg) {
                    consumer(msg.content.toString());
                }

                return {
                    close: function () {
                        ch.close();
                    }
                };
            }));
        }

        return {

            getQueuedEventReceiver: function (endpoint, consumer) {
                return createEventReceiver(endpoint, consumer, false);
            },

            getBroadcastEventReceiver: function (endpoint, consumer) {
                return createEventReceiver(endpoint, consumer, true);
            },

            getRpcServer: function (endpoint, func) {
                return when(conn.createChannel().then(function (ch) {
                    ch.assertExchange(endpoint, 'fanout', {
                        durable: false,
                        autoDelete: true
                    }).then(function () {
                        //exchange exists
                        return ch.assertQueue(endpoint + "_rpcQueue", {
                            exclusive: false,
                            durable: false,
                            autoDelete: true
                        });
                    }).then(function (queueok) {
                        return ch.bindQueue(queueok.queue, endpoint, '');//no routing pattern. Everything is forwarded
                    }).then(function (queueBound) {
                        ch.prefetch(1);
                        return ch.consume(queueBound.queue, handleMessage, {noAck: true});
                    }).then(function () {
                        return true;
                    }).catch(function (error) {
                        console.log(error);
                        return false;
                    });
                    function handleMessage(msg) {
                        var response = func(msg.content.toString());
                        ch.sendToQueue(msg.properties.replyTo,
                            new Buffer(response.toString()),
                            {correlationId: msg.properties.correlationId});
                    }

                    return {
                        close: function () {
                            ch.close();
                        }
                    };
                }));
            },

            getEventEmitter: function () {
                return {
                    emit: function (endpoint, message) {
                        conn.createChannel().then(function (ch) {
                            var dom = domain.create();
                            var result = defer();
                            dom.on('error', function (err) {
                                result.reject(err);
                            });
                            dom.run(function () {
                                ch.publish(endpoint, '', new Buffer(message), {
                                    expiration: options.globalTtl,
                                    persistent: false
                                });
                                try {
                                    ch.close();
                                } catch (err) {
                                    //channel is already closed
                                }
                                result.resolve(true);
                            });
                            return result.promise;
                        });
                    }
                };
            },

            getRpcClient: function () {

                return {
                    call: function (endpoint, message, timeout) {
                        var answer = defer();
                        var dom = domain.create();
                        dom.on('error', function (err) {
                            answer.reject(err);
                        });

                        dom.run(function () {
                            console.log('calling rpc');
                            if (timeout != undefined && timeout != null && isInteger(timeout) && timeout > options.globalTtl) {
                                throw new Error('The timeout must be lower than the configured globalTtl. Currently set to ' + options.globalTtl);
                            }
                            conn.createChannel().then(function (ch) {
                                var corrId = uuid();

                                function maybeAnswer(msg) {
                                    if (msg.properties.correlationId === corrId) {
                                        answer.resolve(msg.content.toString());
                                    }
                                }

                                return ch.assertQueue('', {exclusive: true})
                                    .then(function (qok) {
                                        return qok.queue;
                                    }).then(function (queue) {
                                        return ch.consume(queue, maybeAnswer, {noAck: true})
                                            .then(function () {
                                                return queue;
                                            });
                                    }).then(function (queue) {
                                        console.log("before publish");
                                        ch.publish(endpoint, '', new Buffer(message.toString()), {
                                            expiration: options.globalTtl,
                                            correlationId: corrId,
                                            replyTo: queue,
                                            persistent: false
                                        });
                                        console.log("after publish");
                                        answer.promise.timeout(timeout).finally(function (response) {
                                            try {
                                                ch.close();
                                            } catch (err) {
                                                //channel is already closed
                                            }
                                        });
                                    });
                            });
                        });

                        return answer.promise;
                    }
                };
            },

            close: function () {
                return conn.close();
            }
        }
    }).catch(function (err) {
        console.log('error in connection: ' + err);
    });

};