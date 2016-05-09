var simpleamqp = require('./simple-amqp.js');

function printHelp() {
    console.log("Usage: host command destination [payload/reponse]\n" +
        "  host is a string, ex. \"localhost\"" +
        "  command is one of \"emit\" or \"call\"\n" +
        "  destination is a string\n" +
        "  payload is a string\n");

}

if (process.argv.length < 5) {
    printHelp();
    process.exit(1);
}
var host = process.argv[2];
var command = process.argv[3];
var destination = process.argv[4];
var payload = process.argv[5];

simpleamqp.createEndpointFactory({host: host}, function (err, endpointFactory) {
    if(err) {
        console.log("Failed to create a connection to the broker:");
        console.log(err);
        return;
    }

    function printCallback(err,errMsg,successMsg) {
        if(err) {
            console.log(errMsg);
            console.log(err);
        } else {
            if(successMsg) {
                console.log(successMsg);
            }
        }
    }
    if (command === "emit") {
        endpointFactory.getEventEmitter().emit(destination, payload, function(err,ok) {
                printCallback(err,'event emit failed','event emitted');
            });
    } else if (command === "call") {
        endpointFactory.getRpcClient().call(destination, payload, 59000, function(err, ok) {
                if(err) {
                    console.log('call failed');
                    console.log(err);
                } else {
                    console.log(ok);
                }
            });
    } else if (command === 'listenBroadcast') {
        endpointFactory.getBroadcastEventReceiver(destination, function (err,eventMessage) {
            printCallback(err,'broadcast listen failed', 'Received event: \n' + eventMessage);
        });
    } else if (command === 'listenQueue') {
        endpointFactory.getQueuedEventReceiver(destination, function (err, eventMessage) {
            printCallback(err,'queue listen failed', 'Received event: \n' + eventMessage);
        });
    } else if (command === 'listenRpc') {
        endpointFactory.getRpcServer(destination, function (err, rpcArgument) {
            if(err) {
                console.log('rpc listen failed');
                console.log(err);
            } else {
                console.log('Received rpc call with argument: \n' + rpcArgument + '\nreturning:\n'+payload);
                return payload;
            }
        });
    } else {
        throw new Error('The command provided ("' + command + '") can not be interpreted. Must be either "call" or "emit"');
    }
});
