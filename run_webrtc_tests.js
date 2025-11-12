const WebSocket = require('ws');
const { spawn } = require('child_process');
const path = require('path');

async function startServer() {
    return new Promise((resolve, reject) => {
        const server = spawn('node', ['server.js'], {
            stdio: 'inherit',
            env: { ...process.env, NODE_ENV: 'development' }
        });

        // Give server time to start
        setTimeout(() => {
            resolve(server);
        }, 2000);

        server.on('error', reject);
    });
}

async function waitForServerReady() {
    return new Promise((resolve, reject) => {
        const maxAttempts = 10;
        let attempts = 0;
        
        const tryConnect = () => {
            attempts++;
            const ws = new WebSocket('ws://localhost:8080');
            
            ws.on('open', () => {
                ws.close();
                resolve();
            });
            
            ws.on('error', () => {
                if (attempts >= maxAttempts) {
                    reject(new Error('Server not responding after multiple attempts'));
                } else {
                    setTimeout(tryConnect, 1000);
                }
            });
        };
        
        tryConnect();
    });
}

async function runTests() {
    let server;
    try {
        console.log('Starting server...');
        server = await startServer();
        
        console.log('Waiting for server to be ready...');
        await waitForServerReady();
        
        console.log('Server is ready, running WebRTC signaling tests...');
        const testClient = spawn('node', ['test_webrtc_signaling.js'], {
            stdio: 'inherit'
        });
        
        await new Promise((resolve, reject) => {
            testClient.on('close', (code) => {
                if (code === 0) {
                    resolve();
                } else {
                    reject(new Error(`Test client exited with code ${code}`));
                }
            });
        });
        
    } catch (error) {
        console.error('Test failed:', error);
        process.exit(1);
    } finally {
        if (server) {
            server.kill();
        }
    }
}

runTests();