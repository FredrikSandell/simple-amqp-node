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

simpleamqp.createEndpointFactory({host: host}).then(function (endpointFactory) {
    if (command === "emit") {
        endpointFactory.getEventEmitter().emit(destination, payload).then(function() {
            console.log("emitted event");
        });
    } else if (command === "call") {
        endpointFactory.getRpcClient().call(destination, payload, 59000)
            .then(function (reply) {
                console.log('Got reply:');
                console.log(reply);
            }).catch(function (error) {
                console.log("aaaa");
                console.log(error);
            });
    } else if (command === 'listenBroadcast') {
        endpointFactory.getBroadcastEventReceiver(destination, function (eventMessage) {
            console.log('Received event: \n' + eventMessage);
        });
    } else if (command === 'listenQueue') {
        endpointFactory.getQueuedEventReceiver(destination, function (eventMessage) {
            console.log('Received event: \n' + eventMessage);
        });
    } else if (command === 'listenRpc') {
        endpointFactory.getRpcServer(destination, function (rpcArgument) {
            console.log('Received rpc call with argument: \n' + rpcArgument + '\nreturning:\n'+payload);
            return payload;
        });
    } else {
        throw new Error('The command provided ("' + command + '") can not be interpreted. Must be either "call" or "emit"');
    }
});
