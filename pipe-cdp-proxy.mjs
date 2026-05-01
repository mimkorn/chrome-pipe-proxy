#!/usr/bin/env node
/**
 * pipe-cdp-proxy — multi-client WebSocket front for a single pipe-connected Chrome
 *
 * Lets multiple chrome-devtools-mcp instances (one per Claude Code session) share
 * one Chrome instance via --browserUrl. CDP traffic to Chrome flows over stdio
 * pipes (--remote-debugging-pipe), bypassing the macOS NSApplication-activation
 * bug that affects WebSocket-CDP transport (chrome-devtools-mcp#1254).
 *
 * Architecture:
 *
 *   [MCP-A] ─┐
 *   [MCP-B] ─┼── WebSocket(localhost:9410) ── Pipe Multiplexer ── stdio ── [Chrome]
 *   [MCP-C] ─┘
 *
 * Per-client request ID remapping prevents collisions; per-session ownership
 * keeps events scoped to the client that created the session.
 */

import http from 'http';
import { spawn } from 'child_process';
import { WebSocketServer, WebSocket } from 'ws';

const args = process.argv.slice(2);
const getArg = (name, def) => {
    const i = args.indexOf(name);
    return i >= 0 ? args[i + 1] : def;
};

const PROXY_PORT = parseInt(getArg('--port', '9410'));
const CHROME_PATH = getArg(
    '--chrome-path',
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
);
const USER_DATA_DIR = getArg(
    '--user-data-dir',
    `${process.env.HOME}/Library/Application Support/Chrome-Pipe-Proxy`
);

// ═══════════════════════════════════════════
// Chrome process management
// ═══════════════════════════════════════════

let chromeProcess = null;
let chromeWritable = null;     // fd 3: parent → Chrome
let chromeReadable = null;     // fd 4: Chrome → parent
let chromeBuffer = Buffer.alloc(0);
let restartTimer = null;

function launchChrome() {
    if (chromeProcess) return;
    console.log(`[Proxy] Launching Chrome (--user-data-dir=${USER_DATA_DIR})`);

    chromeProcess = spawn(CHROME_PATH, [
        `--user-data-dir=${USER_DATA_DIR}`,
        '--remote-debugging-pipe',
        '--no-first-run',
        '--no-default-browser-check',
        '--disable-features=DialMediaRouteProvider',
    ], {
        stdio: ['ignore', 'ignore', 'inherit', 'pipe', 'pipe'],
    });

    chromeWritable = chromeProcess.stdio[3];
    chromeReadable = chromeProcess.stdio[4];

    chromeReadable.on('data', handleChromeData);
    chromeReadable.on('error', (err) =>
        console.error('[Proxy] Chrome read error:', err.message)
    );

    let launchedAt = Date.now();
    chromeProcess.on('exit', (code, signal) => {
        const lifetime = Date.now() - launchedAt;
        console.error(
            `[Proxy] Chrome exited (code=${code}, signal=${signal}, lived ${lifetime}ms)`
        );
        const wasRunning = chromeProcess !== null;
        chromeProcess = null;
        chromeWritable = null;
        chromeReadable = null;
        chromeBuffer = Buffer.alloc(0);
        sessionOwners.clear();
        for (const [, req] of pendingRequests) {
            safeSend(req.clientWs, {
                id: req.originalId,
                error: { code: -32000, message: 'Chrome exited' },
            });
        }
        pendingRequests.clear();
        // Don't auto-restart Chrome that died too fast — it's a config error,
        // not a transient failure. Looping would spam windows / processes.
        if (lifetime < 3000) {
            console.error(
                '[Proxy] Chrome died too fast; not auto-restarting (likely a launch-flag/profile error)'
            );
            return;
        }
        if (wasRunning && !restartTimer) {
            restartTimer = setTimeout(() => {
                restartTimer = null;
                launchChrome();
            }, 2000);
        }
    });

    chromeProcess.on('error', (err) =>
        console.error('[Proxy] Chrome spawn error:', err.message)
    );
}

function handleChromeData(chunk) {
    chromeBuffer = Buffer.concat([chromeBuffer, chunk]);
    let nullIdx;
    while ((nullIdx = chromeBuffer.indexOf(0)) !== -1) {
        const msgBuf = chromeBuffer.subarray(0, nullIdx);
        chromeBuffer = chromeBuffer.subarray(nullIdx + 1);
        if (msgBuf.length === 0) continue;
        try {
            const msg = JSON.parse(msgBuf.toString('utf-8'));
            routeChromeMessage(msg);
        } catch (e) {
            console.error('[Proxy] Failed to parse Chrome message:', e.message);
        }
    }
}

function sendToChrome(msg) {
    if (!chromeWritable) return false;
    chromeWritable.write(JSON.stringify(msg) + '\0');
    return true;
}

// ═══════════════════════════════════════════
// Multi-client routing
// ═══════════════════════════════════════════

let globalIdCounter = 1;
const pendingRequests = new Map();   // proxyId → { clientWs, originalId, method, createdAt }
const sessionOwners = new Map();     // sessionId → clientWs
const clientState = new Map();       // clientWs → { sessions: Set, proxyIds: Set, attachedTargets: Set }

function getOrCreateState(clientWs) {
    if (!clientState.has(clientWs)) {
        clientState.set(clientWs, {
            sessions: new Set(),
            proxyIds: new Set(),
            attachedTargets: new Set(),
        });
    }
    return clientState.get(clientWs);
}

function safeSend(ws, data) {
    if (!ws) return;
    if (typeof ws.send !== 'function') return;
    if (ws.readyState !== undefined && ws.readyState !== WebSocket.OPEN) return;
    ws.send(typeof data === 'string' ? data : JSON.stringify(data));
}

function routeChromeMessage(msg) {
    // Response: route by id
    if (msg.id !== undefined && pendingRequests.has(msg.id)) {
        const { clientWs, originalId, method } = pendingRequests.get(msg.id);
        pendingRequests.delete(msg.id);
        const state = clientState.get(clientWs);
        if (state) state.proxyIds.delete(msg.id);

        // Track session attachments for routing
        if (method === 'Target.attachToTarget' && msg.result?.sessionId) {
            const sid = msg.result.sessionId;
            sessionOwners.set(sid, clientWs);
            if (state) state.sessions.add(sid);
        }

        msg.id = originalId;
        safeSend(clientWs, msg);
        return;
    }

    // Event: route by sessionId, or broadcast Target.* events
    if (msg.method) {
        if (msg.sessionId && sessionOwners.has(msg.sessionId)) {
            safeSend(sessionOwners.get(msg.sessionId), msg);
            return;
        }
        // Browser-level events without sessionId — broadcast (rare)
        for (const [ws] of clientState) safeSend(ws, msg);
        return;
    }
}

// Stale-request reaper
setInterval(() => {
    const now = Date.now();
    for (const [proxyId, req] of pendingRequests) {
        if (now - req.createdAt > 60000) {
            safeSend(req.clientWs, {
                id: req.originalId,
                error: { code: -32000, message: 'CDP request timeout (60s)' },
            });
            pendingRequests.delete(proxyId);
            const state = clientState.get(req.clientWs);
            if (state) state.proxyIds.delete(proxyId);
        }
    }
}, 30000);

// ═══════════════════════════════════════════
// Internal CDP request (for HTTP discovery)
// ═══════════════════════════════════════════

function cdpRequest(method, params = {}) {
    return new Promise((resolve, reject) => {
        if (!chromeWritable) return reject(new Error('Chrome not running'));
        const proxyId = globalIdCounter++;
        const timeout = setTimeout(() => {
            pendingRequests.delete(proxyId);
            reject(new Error('timeout'));
        }, 5000);
        const fakeClient = {
            send: (data) => {
                clearTimeout(timeout);
                resolve(typeof data === 'string' ? JSON.parse(data) : data);
            },
            readyState: WebSocket.OPEN,
        };
        pendingRequests.set(proxyId, {
            clientWs: fakeClient,
            originalId: proxyId,
            method,
            createdAt: Date.now(),
        });
        sendToChrome({ id: proxyId, method, params });
    });
}

// ═══════════════════════════════════════════
// HTTP discovery endpoints
// ═══════════════════════════════════════════

const server = http.createServer(async (req, res) => {
    try {
        if (req.url === '/json/version') {
            // Mimic Chrome's /json/version so chrome-devtools-mcp's --browserUrl probe succeeds
            let version = {};
            if (chromeWritable) {
                try {
                    const result = await cdpRequest('Browser.getVersion');
                    version = result.result || {};
                } catch (e) { /* fall through with empty */ }
            }
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
                Browser: version.product || 'Chrome (via Pipe CDP Proxy)',
                'Protocol-Version': version.protocolVersion || '1.3',
                'User-Agent': version.userAgent || '',
                'V8-Version': version.jsVersion || '',
                'WebKit-Version': version.product || '',
                webSocketDebuggerUrl: `ws://127.0.0.1:${PROXY_PORT}/devtools/browser/proxy`,
            }));
            return;
        }

        if (req.url === '/json/list' || req.url === '/json') {
            if (!chromeWritable) {
                res.writeHead(503);
                res.end('Chrome not running');
                return;
            }
            try {
                const result = await cdpRequest('Target.getTargets');
                const targets = (result.result?.targetInfos || []).map((t) => ({
                    ...t,
                    id: t.targetId,
                    description: '',
                    devtoolsFrontendUrl: '',
                    webSocketDebuggerUrl: `ws://127.0.0.1:${PROXY_PORT}/devtools/page/${t.targetId}`,
                }));
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(targets));
            } catch (e) {
                res.writeHead(500);
                res.end(e.message);
            }
            return;
        }

        if (req.url === '/proxy/status') {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
                chromeRunning: chromeProcess !== null,
                chromePid: chromeProcess?.pid || null,
                clients: clientState.size,
                sessions: sessionOwners.size,
                pendingRequests: pendingRequests.size,
            }, null, 2));
            return;
        }

        res.writeHead(404);
        res.end('Not found');
    } catch (e) {
        res.writeHead(500);
        res.end(e.message);
    }
});

// ═══════════════════════════════════════════
// WebSocket server (CDP traffic from MCP clients)
// ═══════════════════════════════════════════

const wss = new WebSocketServer({ server });

wss.on('connection', (clientWs) => {
    const state = getOrCreateState(clientWs);
    console.log(`[Proxy] Client connected (total: ${clientState.size})`);

    clientWs.on('message', (data) => {
        if (!chromeWritable) {
            try {
                const msg = JSON.parse(data.toString());
                if (msg.id !== undefined) {
                    safeSend(clientWs, {
                        id: msg.id,
                        error: { code: -1, message: 'Chrome not running' },
                    });
                }
            } catch (_) { /* ignore */ }
            return;
        }

        try {
            const msg = JSON.parse(data.toString());

            // ID remap so multiple clients don't collide
            if (msg.id !== undefined) {
                const proxyId = globalIdCounter++;
                pendingRequests.set(proxyId, {
                    clientWs,
                    originalId: msg.id,
                    method: msg.method,
                    createdAt: Date.now(),
                });
                state.proxyIds.add(proxyId);
                msg.id = proxyId;
            }

            sendToChrome(msg);
        } catch (e) {
            console.error('[Proxy] Bad client message:', e.message);
        }
    });

    const cleanup = () => {
        for (const sid of state.sessions) sessionOwners.delete(sid);
        for (const pid of state.proxyIds) pendingRequests.delete(pid);
        clientState.delete(clientWs);
        console.log(`[Proxy] Client disconnected (remaining: ${clientState.size})`);
    };

    clientWs.on('close', cleanup);
    clientWs.on('error', cleanup);
});

// ═══════════════════════════════════════════
// Lifecycle
// ═══════════════════════════════════════════

process.on('uncaughtException', (err) =>
    console.error('[Proxy] Uncaught exception:', err.message, err.stack)
);
process.on('unhandledRejection', (reason) =>
    console.error('[Proxy] Unhandled rejection:', reason)
);

function shutdown() {
    console.log('[Proxy] Shutting down');
    try { chromeProcess?.kill('SIGTERM'); } catch (_) {}
    process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

server.listen(PROXY_PORT, '127.0.0.1', () => {
    console.log(`[Proxy] Listening on http://127.0.0.1:${PROXY_PORT}`);
    launchChrome();
});
