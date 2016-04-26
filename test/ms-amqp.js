var proxyquire = require('proxyquire');
var assert = require('assert');
var when = require('when');
//var simpleamqp = require('../simple-amqp.js');

var amqplib_stub = {
    exampleFunc: function () {
    }
};

var simple_amqp = proxyquire('../simple-amqp.js', {
    'amqplib': amqplib_stub
});

describe('createEndpointFactory', function () {
/*
    it('given a valid option object a valid endpoint factory should be returned', function (done) {
        amqplib_stub.connect = function(hostUrl) {
            assert.equal('amqp://localhost',hostUrl);
            return when('dummyConnection');
        };

        simple_amqp.createEndpointFactory({
            host: 'localhost'
        }).then(function(endpointFactory) {
            assert.equal('function', typeof endpointFactory.getQueuedEventReceiver);
            assert.equal('function', typeof endpointFactory.getBroadcastEventReceiver);
            assert.equal('function', typeof endpointFactory.getRpcServer);
            assert.equal('function', typeof endpointFactory.getEventEmitter);
            assert.equal('function', typeof endpointFactory.getRpcClient);
            done();
        });
    });*/

    /*
    it('given a valid option object a valid endpoint factory should be returned', function (done) {
        amqplib_stub.connect = function(hostUrl) {
            assert.equal('amqp://localhost',hostUrl);
            return when('dummyConnection');
        };

        simple_amqp.createEndpointFactory({
            host: 'localhost'
        }).then(function(endpointFactory) {
            assert.equal('function', typeof endpointFactory.getQueuedEventReceiver);
            assert.equal('function', typeof endpointFactory.getBroadcastEventReceiver);
            assert.equal('function', typeof endpointFactory.getRpcServer);
            assert.equal('function', typeof endpointFactory.getEventEmitter);
            assert.equal('function', typeof endpointFactory.getRpcClient);
            done();
        });
    });

    it('given an invalid option object an exception should be thrown', function (done) {
        amqplib_stub.connect = function(hostUrl) {
            assert.equal('amqp://localhost',hostUrl);
            return when('dummyConnection');
        };

        simple_amqp.createEndpointFactory({
        }).catch(function (error) {
            assert.equal('object', typeof error)
            done();
        });
    });

    describe('#getEventEmitter()', function () {

        it('should return a object capable of emitting events', function (done) {
            var channel_stub = {};
            channel_stub.assertExchange = function(endpoint, type, opts) {
                console.log('calling assert exchange');
                assert.equal('/the/endpoint',endpoint);
                assert.equal('fanout',type);
                return when({});
            };

            channel_stub.publish = function(endpoint, routingKey, message, options) {
                
            };
            var isClosed = false;
            channel_stub.close = function() {
                isClosed = true;
            };

            var connection_stub = {
                createChannel : function() {
                    console.log('calling create channel');
                    return when(channel_stub);
                }
            };
            amqplib_stub.connect = function(url) {
                console.log('creating connection');
                return when(connection_stub);
            };

            channel_stub.


            simple_amqp.createEndpointFactory({
                host:'localhost'
            }).then(function(endpointFactory) {
                endpointFactory.getEventEmitter().emit('/the/endpoint','the_message').then(function() {
                    console.log("emitted the event");
                    done();
                });
            });


        });
    });*/
});