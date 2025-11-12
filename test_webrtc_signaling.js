const WebSocket = require('ws');
const readline = require('readline');

// Test users
const user1 = { id: 'test_user_1', name: 'Alice' };
const user2 = { id: 'test_user_2', name: 'Bob' };

// WebSocket connections for both users
let ws1, ws2;

// Call state
let currentCallId = null;

// Connect both users to the signaling server
function connectUsers() {
    return new Promise((resolve, reject) => {
        ws1 = new WebSocket('ws://localhost:8080');
        ws2 = new WebSocket('ws://localhost:8080');

        let connected = 0;
        const onOpen = () => {
            connected++;
            if (connected === 2) resolve();
        };

        ws1.on('open', onOpen);
        ws2.on('open', onOpen);

        ws1.on('error', reject);
        ws2.on('error', reject);

        // Handle incoming messages for user1
        ws1.on('message', (message) => {
            try {
                const data = JSON.parse(message);
                console.log(`[${user1.name}] Received:`, data);
                
                // Auto-respond to call offer
                if (data.type === 'call:offer') {
                    setTimeout(() => {
                        sendAnswer(ws1, data.data.callId);
                    }, 1000);
                }
            } catch (e) {
                console.error('Error parsing message:', e);
            }
        });

        // Handle incoming messages for user2
        ws2.on('message', (message) => {
            try {
                const data = JSON.parse(message);
                console.log(`[${user2.name}] Received:`, data);

                // Auto-send ICE candidates after answering
                if (data.type === 'call:answer') {
                    setTimeout(() => {
                        sendIceCandidate(ws2, data.data.callId);
                    }, 500);
                }
            } catch (e) {
                console.error('Error parsing message:', e);
            }
        });
    });
}

// Authenticate users
async function authenticateUsers() {
    const auth1 = new Promise((resolve) => {
        ws1.send(JSON.stringify({
            type: 'auth',
            userId: user1.id
        }));
        ws1.once('message', resolve);
    });

    const auth2 = new Promise((resolve) => {
        ws2.send(JSON.stringify({
            type: 'auth',
            userId: user2.id
        }));
        ws2.once('message', resolve);
    });

    await Promise.all([auth1, auth2]);
    console.log('Both users authenticated');
}

// Simulate starting a call
function startCall() {
    currentCallId = `test_call_${Date.now()}`;
    
    // Send call init
    ws1.send(JSON.stringify({
        type: 'call:init',
        fromId: user1.id,
        toId: user2.id,
        callId: currentCallId
    }));

    // Send offer after short delay
    setTimeout(() => {
        ws1.send(JSON.stringify({
            type: 'call:offer',
            fromId: user1.id,
            toId: user2.id,
            callId: currentCallId,
            data: {
                sdp: 'v=0\\r\\no=- 123 2 IN IP4 127.0.0.1\\r\\ns=-\\r\\nt=0 0\\r\\na=group:BUNDLE audio\\r\\n'
            }
        }));
    }, 500);
}

// Simulate answering a call
function sendAnswer(ws, callId) {
    ws.send(JSON.stringify({
        type: 'call:answer',
        fromId: user2.id,
        toId: user1.id,
        callId: callId,
        data: {
            sdp: 'v=0\\r\\no=- 456 2 IN IP4 127.0.0.1\\r\\ns=-\\r\\nt=0 0\\r\\na=group:BUNDLE audio\\r\\n'
        }
    }));
}

// Simulate ICE candidate exchange
function sendIceCandidate(ws, callId) {
    ws.send(JSON.stringify({
        type: 'call:ice',
        fromId: user2.id,
        toId: user1.id,
        callId: callId,
        data: {
            candidate: 'candidate:1 1 UDP 2122252543 192.168.1.2 50041 typ host',
            sdpMLineIndex: 0
        }
    }));
}

// End current call
function endCall() {
    if (!currentCallId) return;
    
    ws1.send(JSON.stringify({
        type: 'call:end',
        fromId: user1.id,
        toId: user2.id,
        callId: currentCallId
    }));

    currentCallId = null;
}

// Main test sequence
async function runTest() {
    try {
        console.log('Connecting users...');
        await connectUsers();
        console.log('Users connected');

        console.log('Authenticating users...');
        await authenticateUsers();

        // Setup CLI interface
        const rl = readline.createInterface({
            input: process.stdin,
            output: process.stdout
        });

        console.log('\nCommands:');
        console.log('1 - Start call');
        console.log('2 - End call');
        console.log('3 - Send ICE candidate');
        console.log('q - Quit');

        rl.on('line', (input) => {
            switch(input.trim()) {
                case '1':
                    console.log('Starting call...');
                    startCall();
                    break;
                case '2':
                    console.log('Ending call...');
                    endCall();
                    break;
                case '3':
                    if (currentCallId) {
                        console.log('Sending ICE candidate...');
                        sendIceCandidate(ws1, currentCallId);
                    } else {
                        console.log('No active call');
                    }
                    break;
                case 'q':
                    console.log('Closing connections...');
                    ws1.close();
                    ws2.close();
                    rl.close();
                    break;
                default:
                    console.log('Unknown command');
            }
        });

    } catch (error) {
        console.error('Test failed:', error);
        process.exit(1);
    }
}

// Start the test
runTest();