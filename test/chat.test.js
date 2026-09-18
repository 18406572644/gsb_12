'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { DatabaseSync } = require('node:sqlite');
const WebSocket = require('ws');
const { createChatServer } = require('../src/server');
const { ChatDB } = require('../src/db');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** 构造一个 node:sqlite 风格的忙锁错误（ERR_SQLITE_ERROR + database is locked） */
function busyError() {
  const e = new Error('database is locked');
  e.code = 'ERR_SQLITE_ERROR';
  return e;
}

/**
 * 让对象的某个方法前 failCount 次抛出忙锁错误，之后恢复原行为。
 * 用于在没有真实跨连接锁竞争时，确定性地注入 SQLITE_BUSY。
 */
function failBusyFirst(obj, method, failCount) {
  const original = obj[method].bind(obj);
  let left = failCount;
  obj[method] = (...args) => {
    if (left > 0) {
      left--;
      throw busyError();
    }
    return original(...args);
  };
  return () => { obj[method] = original; };
}

/** 启动一个隔离的测试服务器（内存库、随机端口、默认关闭重发以免干扰计数） */
async function startServer(overrides = {}) {
  const server = createChatServer({
    port: 0,
    host: '127.0.0.1',
    dbPath: ':memory:',
    heartbeatIntervalMs: 60_000,
    ackResendAfterMs: 60_000, // 默认不在测试内重发；重发场景单独配置
    ...overrides,
  });
  const addr = await server.start();
  return { server, port: addr.port };
}

async function login(port, name) {
  const res = await fetch(`http://127.0.0.1:${port}/api/login`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', connection: 'close' },
    body: JSON.stringify({ name }),
  });
  assert.equal(res.status, 200);
  return res.json();
}

/** 测试客户端：手动 ACK（测试可控）。log 全量记录供断言；waitFor 消费式匹配（每帧至多满足一个等待者） */
class Client {
  static async connect(port, token) {
    const c = new Client();
    c.log = []; // 全部帧（断言用）
    c.pending = []; // 未被 waitFor 消费的帧
    c.waiters = [];
    c.closed = new Promise((res) => (c._onClosed = res));
    c.ws = new WebSocket(`ws://127.0.0.1:${port}/ws?token=${encodeURIComponent(token)}`);
    c.ws.on('message', (raw) => {
      const m = JSON.parse(raw.toString());
      c.log.push(m);
      for (const w of [...c.waiters]) {
        if (w.pred(m)) {
          c.waiters.splice(c.waiters.indexOf(w), 1);
          clearTimeout(w.timer);
          w.resolve(m);
          return;
        }
      }
      c.pending.push(m);
    });
    c.ws.on('close', () => c._onClosed());
    await new Promise((res, rej) => {
      c.ws.once('open', res);
      c.ws.once('error', rej);
    });
    await c.waitFor((m) => m.type === 'welcome');
    return c;
  }

  send(obj) {
    this.ws.send(JSON.stringify(obj));
  }

  waitFor(pred, timeout = 3000) {
    const idx = this.pending.findIndex(pred);
    if (idx >= 0) {
      const [m] = this.pending.splice(idx, 1);
      return Promise.resolve(m);
    }
    return new Promise((resolve, reject) => {
      const w = { pred, resolve };
      w.timer = setTimeout(() => {
        reject(new Error('waitFor: timed out'));
      }, timeout);
      this.waiters.push(w);
    });
  }

  /** 已收到的某房间消息帧（seq 列表） */
  roomSeqs(roomId) {
    return this.log.filter((m) => m.type === 'msg' && m.roomId === roomId).map((m) => m.seq);
  }

  close() {
    this.ws.close();
    return this.closed;
  }
}

async function createRoom(client, name) {
  client.send({ type: 'create_room', name });
  const joined = await client.waitFor((m) => m.type === 'joined' && m.name === name);
  return joined.roomId;
}

async function joinRoom(client, room, lastSeq = 0) {
  client.send({ type: 'join', room, lastSeq });
  return client.waitFor((m) => m.type === 'joined');
}

// ---------------------------------------------------------------- 测试用例

test('登录、连接、建房后成为管理员', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'alice');
    assert.ok(u.userId && u.token);
    const a = await Client.connect(port, u.token);
    const roomId = await createRoom(a, 'general');
    const joined = a.log.find((m) => m.type === 'joined');
    assert.equal(joined.role, 'admin');
    assert.ok(roomId);
    await a.close();
  } finally {
    server.stop();
  }
});

test('发送收到 ACK，房间内广播按 seq 全序投递', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    for (let i = 1; i <= 3; i++) {
      a.send({ type: 'msg', roomId, clientMsgId: `m${i}`, content: `hello ${i}` });
    }
    // 发送者收到 3 个 ACK，seq 递增
    for (let i = 1; i <= 3; i++) {
      const ack = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === `m${i}`);
      assert.equal(ack.seq, i);
    }
    // 接收者按序收到 1,2,3
    await b.waitFor((m) => m.type === 'msg' && m.roomId === roomId && m.seq === 3);
    assert.deepEqual(b.roomSeqs(roomId), [1, 2, 3]);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('重复 clientMsgId 幂等：返回同一 seq，不重复广播', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'dup-1', content: 'hello' });
    const ack1 = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'dup-1');
    // 网络重试：同 clientMsgId 重发
    a.send({ type: 'msg', roomId, clientMsgId: 'dup-1', content: 'hello' });
    const ack2 = await a.waitFor(
      (m) => m.type === 'ack' && m.clientMsgId === 'dup-1' && m !== ack1
    );
    assert.equal(ack1.seq, ack2.seq);

    await b.waitFor((m) => m.type === 'msg' && m.roomId === roomId);
    await sleep(300);
    assert.deepEqual(b.roomSeqs(roomId), [1], '接收端只应收到一次广播');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('断线补发：重连后按序补齐离线期间的消息，且不重复', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'online' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    await b.close(); // —— B 掉线 ——

    for (const [i, c] of [2, 3, 4].entries()) {
      a.send({ type: 'msg', roomId, clientMsgId: `m${i + 2}`, content: `offline ${c}` });
    }
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'm4');

    // —— B 重连，携带本地进度 lastSeq=1 ——
    b = await Client.connect(port, ub.token);
    await joinRoom(b, roomId, 1);
    await b.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
    assert.deepEqual(b.roomSeqs(roomId), [2, 3, 4], '补发且仅补发缺口，按序到达');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('已追平的连接重连后不再收到旧消息', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'hi' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    await b.close();

    b = await Client.connect(port, ub.token);
    await joinRoom(b, roomId, 1); // 已追平
    await sleep(300);
    assert.deepEqual(b.roomSeqs(roomId), [], '不应有任何补发');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('服务端对未 ACK 消息重发，ACK 后停止', async () => {
  const { server, port } = await startServer({
    ackResendIntervalMs: 50,
    ackResendAfterMs: 100,
    ackMaxResend: 10,
  });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'hi' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    // 不 ACK，等服务端重发
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1, 2000);
    assert.ok(b.roomSeqs(roomId).length >= 2, '应观察到至少一次重发');

    b.send({ type: 'ack', roomId, seq: 1 });
    await sleep(100);
    const countAfterAck = b.roomSeqs(roomId).length;
    await sleep(400);
    assert.equal(b.roomSeqs(roomId).length, countAfterAck, 'ACK 后不应再有重发');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('禁言：管理员可禁言/解禁，被禁言者发送被拒', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'mute', roomId, userId: ub.userId, minutes: 10 });
    await b.waitFor((m) => m.type === 'notice' && m.event === 'muted' && m.userId === ub.userId);

    b.send({ type: 'msg', roomId, clientMsgId: 'x1', content: 'am i muted?' });
    const err = await b.waitFor((m) => m.type === 'error');
    assert.equal(err.code, 'MUTED');

    a.send({ type: 'unmute', roomId, userId: ub.userId });
    await b.waitFor((m) => m.type === 'notice' && m.event === 'unmuted');

    b.send({ type: 'msg', roomId, clientMsgId: 'x2', content: 'free again' });
    const ack = await b.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'x2');
    assert.equal(ack.seq, 1);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('权限：普通成员不能禁言他人，管理员不可被禁言', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const uc = await login(port, 'carol');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const c = await Client.connect(port, uc.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);
    await joinRoom(c, roomId);

    b.send({ type: 'mute', roomId, userId: uc.userId, minutes: 5 });
    const err1 = await b.waitFor((m) => m.type === 'error');
    assert.equal(err1.code, 'FORBIDDEN');

    a.send({ type: 'mute', roomId, userId: ua.userId, minutes: 5 });
    const err2 = await a.waitFor((m) => m.type === 'error');
    assert.equal(err2.code, 'FORBIDDEN');
    await Promise.all([a.close(), b.close(), c.close()]);
  } finally {
    server.stop();
  }
});

test('连接数限制：单用户连接数超限被拒绝', async () => {
  const { server, port } = await startServer({ maxConnectionsPerUser: 2 });
  try {
    const u = await login(port, 'alice');
    const c1 = await Client.connect(port, u.token);
    const c2 = await Client.connect(port, u.token);
    await assert.rejects(
      Client.connect(port, u.token),
      /503|TOO_MANY_DEVICES|Unexpected server response/
    );
    await c1.close();
    await c2.close();
  } finally {
    server.stop();
  }
});

test('发送限流：突发超过令牌桶被拒绝', async () => {
  const { server, port } = await startServer({ rateLimitPerSec: 1, rateLimitBurst: 2 });
  try {
    const u = await login(port, 'alice');
    const a = await Client.connect(port, u.token);
    const roomId = await createRoom(a, 'general');

    for (let i = 0; i < 5; i++) {
      a.send({ type: 'msg', roomId, clientMsgId: `r${i}`, content: `spam ${i}` });
    }
    const err = await a.waitFor((m) => m.type === 'error' && m.code === 'RATE_LIMITED');
    assert.ok(err);
    await sleep(300);
    const ackCount = a.log.filter((m) => m.type === 'ack').length;
    assert.equal(ackCount, 2, '突发容量为 2，其余应被限流');
    await a.close();
  } finally {
    server.stop();
  }
});

test('历史消息分页拉取', async () => {
  const { server, port } = await startServer();
  try {
    const u = await login(port, 'alice');
    const a = await Client.connect(port, u.token);
    const roomId = await createRoom(a, 'general');
    for (let i = 1; i <= 5; i++) {
      a.send({ type: 'msg', roomId, clientMsgId: `h${i}`, content: `msg ${i}` });
    }
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'h5');

    a.send({ type: 'history', roomId, beforeSeq: 4, limit: 2 });
    const h = await a.waitFor((m) => m.type === 'history');
    assert.deepEqual(h.messages.map((m) => m.seq), [2, 3], '升序返回 beforeSeq 之前的一页');
    assert.equal(h.hasMore, true);
    await a.close();
  } finally {
    server.stop();
  }
});

test('服务端游标兜底：新设备不带 lastSeq 时从已确认进度继续', async () => {
  const { server, port } = await startServer();
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    let b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    a.send({ type: 'msg', roomId, clientMsgId: 'm1', content: 'first' });
    await b.waitFor((m) => m.type === 'msg' && m.seq === 1);
    b.send({ type: 'ack', roomId, seq: 1 }); // 上报确认进度
    await sleep(100);
    await b.close();

    a.send({ type: 'msg', roomId, clientMsgId: 'm2', content: 'second' });
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'm2');

    // 新设备重连，不带 lastSeq —— 应使用服务端游标，只补 seq 2
    b = await Client.connect(port, ub.token);
    b.send({ type: 'join', room: roomId });
    await b.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
    assert.deepEqual(b.roomSeqs(roomId), [2]);
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

test('持久化：服务重启后消息不丢失', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-test-'));
  const dbPath = path.join(dir, 'test.db');
  try {
    let token, roomId;
    {
      const { server, port } = await startServer({ dbPath });
      const u = await login(port, 'alice');
      token = u.token;
      const a = await Client.connect(port, token);
      roomId = await createRoom(a, 'persist');
      for (let i = 1; i <= 3; i++) {
        a.send({ type: 'msg', roomId, clientMsgId: `p${i}`, content: `durable ${i}` });
      }
      await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'p3');
      await a.close();
      server.stop();
    }
    {
      const { server, port } = await startServer({ dbPath });
      const a = await Client.connect(port, token); // 同一 token 仍有效
      await joinRoom(a, roomId, 0);
      await a.waitFor((m) => m.type === 'sync_done' && m.roomId === roomId);
      assert.deepEqual(a.roomSeqs(roomId), [1, 2, 3], '重启后历史消息完整可补发');
      await a.close();
      server.stop();
    }
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------- 忙锁重试

/** 直接在 ChatDB 层验证忙锁重试（不经过网络） */
test('ChatDB：读操作忙锁重试后成功', async () => {
  const db = new ChatDB(':memory:', { busyTimeoutMs: 50, maxRetries: 3, backoffBaseMs: 2, backoffMaxMs: 8 });
  try {
    await db.createUser('u1', 'alice', 'r');
    await db.createRoom('r1', 'general', 'u1');

    // 前两次查询忙锁、第三次成功；若未重试则会直接抛错
    const restore = failBusyFirst(db.stmt.member, 'get', 2);
    const member = await db.getMember('r1', 'u1');
    restore();

    assert.equal(member.role, 'admin');
  } finally {
    db.close();
  }
});

test('ChatDB：事务体内忙锁整体重试，seq 不产生空洞', async () => {
  const db = new ChatDB(':memory:', { busyTimeoutMs: 50, maxRetries: 3, backoffBaseMs: 2, backoffMaxMs: 8 });
  try {
    await db.createUser('u1', 'alice', 'r');
    await db.createRoom('r1', 'general', 'u1');

    // bumpSeq 第一次抛忙锁：事务回滚，重试后该消息仍应拿到 seq=1（而不是 2）
    const restore = failBusyFirst(db.stmt.bumpSeq, 'get', 1);
    const { message, duplicate } = await db.insertMessage({
      roomId: 'r1', clientMsgId: 'c1', senderId: 'u1', content: 'x',
    });
    restore();

    assert.equal(duplicate, false);
    assert.equal(message.seq, 1, '回滚重试不应造成 seq 跳号');
    const rows = await db.getMessagesAfter('r1', 0, 100);
    assert.deepEqual(rows.map((r) => r.seq), [1]);
  } finally {
    db.close();
  }
});

test('ChatDB：BEGIN 阶段忙锁重试后提交成功', async () => {
  const db = new ChatDB(':memory:', { busyTimeoutMs: 50, maxRetries: 3, backoffBaseMs: 2, backoffMaxMs: 8 });
  try {
    await db.createUser('u1', 'alice', 'r');
    await db.createRoom('r1', 'general', 'u1');

    let beginCalls = 0;
    const origExec = db.db.exec.bind(db.db);
    db.db.exec = (sql) => {
      if (typeof sql === 'string' && sql.includes('BEGIN') && beginCalls++ < 2) throw busyError();
      return origExec(sql);
    };
    const { message } = await db.insertMessage({
      roomId: 'r1', clientMsgId: 'c1', senderId: 'u1', content: 'x',
    });
    db.db.exec = origExec;

    assert.equal(message.seq, 1);
  } finally {
    db.close();
  }
});

test('ChatDB：游标写忙锁可重试且仅向前推进', async () => {
  const db = new ChatDB(':memory:', { busyTimeoutMs: 50, maxRetries: 3, backoffBaseMs: 2, backoffMaxMs: 8 });
  try {
    await db.createUser('u1', 'alice', 'r');
    await db.createRoom('r1', 'general', 'u1');

    const restore = failBusyFirst(db.stmt.upsertCursor, 'run', 2);
    await db.saveCursor('r1', 'u1', 5);
    restore();

    assert.equal(await db.getCursor('r1', 'u1'), 5);
    await db.saveCursor('r1', 'u1', 3); // 旧进度不能回退
    assert.equal(await db.getCursor('r1', 'u1'), 5);
  } finally {
    db.close();
  }
});

test('ChatDB：退避等待期间不阻塞事件循环', async () => {
  const db = new ChatDB(':memory:', { busyTimeoutMs: 50, maxRetries: 3, backoffBaseMs: 5, backoffMaxMs: 20 });
  try {
    await db.createUser('u1', 'alice', 'r');
    await db.createRoom('r1', 'general', 'u1');

    let ticked = false;
    const timer = setTimeout(() => { ticked = true; }, 1);
    const restore = failBusyFirst(db.stmt.member, 'get', 1); // 触发一次 await sleep 退避
    await db.getMember('r1', 'u1');
    restore();
    clearTimeout(timer);

    assert.ok(ticked, '退避用 setTimeout，事件循环上的定时器应能在等待期间触发');
  } finally {
    db.close();
  }
});

test('ChatDB：忙锁重试耗尽后抛出原始错误；非忙锁错误不重试', async () => {
  const db = new ChatDB(':memory:', { busyTimeoutMs: 50, maxRetries: 2, backoffBaseMs: 1, backoffMaxMs: 2 });
  try {
    await db.createUser('u1', 'alice', 'r');
    await db.createRoom('r1', 'general', 'u1');

    // 持续忙锁 -> 耗尽重试后向上抛
    db.stmt.member.get = () => { throw busyError(); };
    await assert.rejects(() => db.getMember('r1', 'u1'), /database is locked/);

    // 非忙锁错误立即抛出，不重试
    let calls = 0;
    db.stmt.member.get = () => { calls++; throw new Error('disk I/O error'); };
    await assert.rejects(() => db.getMember('r1', 'u1'), /disk I\/O error/);
    assert.equal(calls, 1, '非忙锁错误只应执行一次');
  } finally {
    db.close();
  }
});

/**
 * 端到端：消息写入期间注入忙锁，客户端仍应收到 ACK（服务端自动重试），不返回 INTERNAL。
 */
test('端到端：发消息遇忙锁自动重试成功，seq 连续且无 INTERNAL 错误', async () => {
  const { server, port } = await startServer({
    dbBusyMaxRetries: 4, dbBusyBackoffBaseMs: 2, dbBusyBackoffMaxMs: 8,
  });
  try {
    const ua = await login(port, 'alice');
    const a = await Client.connect(port, ua.token);
    const roomId = await createRoom(a, 'general');

    // 在写第 1 条消息时让 bumpSeq 忙两次
    const restore = failBusyFirst(server.db.stmt.bumpSeq, 'get', 2);
    a.send({ type: 'msg', roomId, clientMsgId: 'busy-1', content: 'retry me' });
    const ack = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'busy-1');
    restore();

    assert.equal(ack.seq, 1, '重试成功后应分配 seq=1');

    // 再发一条，确认 seq 连续无空洞
    a.send({ type: 'msg', roomId, clientMsgId: 'busy-2', content: 'after' });
    const ack2 = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'busy-2');
    assert.equal(ack2.seq, 2);

    await sleep(50);
    const internal = a.log.filter((m) => m.type === 'error' && m.code === 'INTERNAL');
    assert.equal(internal.length, 0, '不应出现 INTERNAL 错误');
    await a.close();
  } finally {
    server.stop();
  }
});

/**
 * 端到端：忙锁重试耗尽时返回 INTERNAL；恢复后后续消息正常（连接不被拖垮）。
 */
test('端到端：忙锁持续不解除时返回 INTERNAL，恢复后服务仍可用', async () => {
  const { server, port } = await startServer({
    dbBusyMaxRetries: 2, dbBusyBackoffBaseMs: 1, dbBusyBackoffMaxMs: 2,
  });
  try {
    const ua = await login(port, 'alice');
    const a = await Client.connect(port, ua.token);
    const roomId = await createRoom(a, 'general');

    // 用开关控制 bumpSeq 是否抛忙锁，便于恢复
    let stuck = true;
    const origBump = server.db.stmt.bumpSeq.get.bind(server.db.stmt.bumpSeq);
    server.db.stmt.bumpSeq.get = (...args) => {
      if (stuck) throw busyError();
      return origBump(...args);
    };

    a.send({ type: 'msg', roomId, clientMsgId: 'stuck', content: 'x' });
    const err = await a.waitFor((m) => m.type === 'error' && m.code === 'INTERNAL');
    assert.ok(err);

    // 恢复后新消息正常；前一条事务整体回滚，seq 仍从 1 开始
    stuck = false;
    a.send({ type: 'msg', roomId, clientMsgId: 'ok', content: 'y' });
    const ack = await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === 'ok');
    assert.equal(ack.seq, 1, '前一条事务回滚，恢复后首条仍为 seq=1');
    await a.close();
  } finally {
    server.stop();
  }
});

/**
 * 并发回归：msg（事务写）/ ack（游标写）/ history（读）同时高频进行，
 * 不应出现 INTERNAL 错误，且房间 seq 连续。
 */
test('并发：消息写入/游标保存/历史查询并发执行无 INTERNAL 错误', async () => {
  const { server, port } = await startServer({
    dbBusyMaxRetries: 6, dbBusyBackoffBaseMs: 2, dbBusyBackoffMaxMs: 16,
    rateLimitPerSec: 1000, rateLimitBurst: 1000, // 测试中放开限流
  });
  try {
    const ua = await login(port, 'alice');
    const ub = await login(port, 'bob');
    const a = await Client.connect(port, ua.token);
    const b = await Client.connect(port, ub.token);
    const roomId = await createRoom(a, 'general');
    await joinRoom(b, roomId);

    const N = 40;
    // 发送者：连续写 N 条
    const sendP = (async () => {
      for (let i = 1; i <= N; i++) {
        a.send({ type: 'msg', roomId, clientMsgId: `cc${i}`, content: `m${i}` });
        if (i % 3 === 0) await sleep(1); // 让出事件循环，制造交错
      }
    })();

    // 接收者：持续 ack（游标写）+ history（读）
    const noiseP = (async () => {
      for (let i = 0; i < N; i++) {
        b.send({ type: 'history', roomId, limit: 10 });
        b.send({ type: 'ack', roomId, seq: i });
        await sleep(1);
      }
    })();

    // 等待最后一条 ACK
    await a.waitFor((m) => m.type === 'ack' && m.clientMsgId === `cc${N}`, 8000);
    await sendP;
    await noiseP;
    await sleep(100);

    const internal = [...a.log, ...b.log].filter((m) => m.type === 'error' && m.code === 'INTERNAL');
    assert.equal(internal.length, 0, '并发期间不应出现 INTERNAL 错误');

    // 发送者侧 seq 连续 1..N
    const seqs = a.log
      .filter((m) => m.type === 'ack' && m.roomId === roomId)
      .map((m) => m.seq)
      .sort((x, y) => x - y);
    assert.deepEqual(seqs, Array.from({ length: N }, (_, i) => i + 1), 'seq 应连续无空洞');
    await a.close();
    await b.close();
  } finally {
    server.stop();
  }
});

/**
 * 真实锁竞争：另一条连接持有写锁期间发消息，服务端应忙锁重试，
 * 锁释放后自动成功；等待期间事件循环不得被长时间阻塞，且不返回 INTERNAL。
 */
test('真实忙锁：他连接持写锁时消息在锁释放后重试成功，事件循环不被长阻塞', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-lock-'));
  const dbPath = path.join(dir, 'lock.db');
  const { server, port } = await startServer({
    dbPath,
    dbBusyTimeoutMs: 10, // 同步忙等上限仅 10ms，到点即抛 busy 交应用层退避
    dbBusyMaxRetries: 40,
    dbBusyBackoffBaseMs: 5,
    dbBusyBackoffMaxMs: 20,
  });
  const locker = new DatabaseSync(dbPath);
  locker.exec('PRAGMA busy_timeout = 0');
  try {
    const ua = await login(port, 'alice');
    const a = await Client.connect(port, ua.token);
    const roomId = await createRoom(a, 'general');

    // 第二连接抢占写锁并持有 ~400ms
    locker.exec('BEGIN IMMEDIATE');
    const t0 = Date.now();
    let loopLatencyMs = null;
    const probe = setTimeout(() => { loopLatencyMs = Date.now() - t0; }, 5);

    a.send({ type: 'msg', roomId, clientMsgId: 'locked-1', content: 'waiting' });
    await sleep(400);

    assert.ok(
      !a.log.some((m) => m.type === 'ack' && m.clientMsgId === 'locked-1'),
      '持锁期间消息不应被确认'
    );
    assert.ok(loopLatencyMs !== null && loopLatencyMs < 200,
      `事件循环未被长阻塞（探针延迟 ${loopLatencyMs}ms，应远小于持锁时长 400ms）`);

    locker.exec('COMMIT'); // 释放写锁
    const ack = await a.waitFor(
      (m) => m.type === 'ack' && m.clientMsgId === 'locked-1', 5000
    );
    clearTimeout(probe);
    assert.equal(ack.seq, 1);
    assert.equal(
      a.log.filter((m) => m.type === 'error' && m.code === 'INTERNAL').length, 0
    );
    await a.close();
  } finally {
    try { locker.exec('COMMIT'); } catch { /* 已提交 */ }
    locker.close();
    server.stop();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
