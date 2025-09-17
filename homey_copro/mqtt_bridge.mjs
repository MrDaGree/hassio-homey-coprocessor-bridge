import mqtt from '/opt/mqtt/node_modules/mqtt/build/index.js';
import Coprocessor from '/app/services/Coprocessor.mjs';
import fs from 'fs';

// Homey Z-Wave (CJS → ESM interop)
import ZwavePkg from '@athombv/zwave';
const ZWave = ZwavePkg?.ZWave || ZwavePkg?.default || ZwavePkg;

// zwave-js config API (CJS path)
import { ConfigManager } from '/opt/mqtt/node_modules/@zwave-js/config/build/cjs/index.js';

// ── Config ────────────────────────────────────────────────────────────────────
const DISC_PREFIX = process.env.MQTT_DISCOVERY_PREFIX || 'homeassistant';
const REGION      = process.env.ZW_REGION || 'US';
const COUNTRY     = process.env.ZW_COUNTRY || 'DEFAULT';

const LOG = (m, ...a) => console.log('[bridge]', m, ...a);

// Controller-wide availability
const AVAIL_TOPIC = `${DISC_PREFIX}/zwave/bridge/availability`;
const CTRL_CMD_TOPIC = `${DISC_PREFIX}/zwave/controller/cmd`;

// ── State ─────────────────────────────────────────────────────────────────────
let mqttClient = null;
let zwave = null;
let shuttingDown = false;

const cfg = new ConfigManager();

const nodeInfoCache = new Map();    // nodeId -> { name, manufacturer, model, ids, devCfg, resolved }
const paramDefsCache = new Map();   // nodeId -> [param defs]
const announcedNodes = new Set();
const nodeResolvePromises = new Map(); // nodeId -> Promise (avoid duplicate resolves)
const nodeSupervisionSid = new Map();  // nodeId -> 0..63 rolling session id

// Pending config writes while node sleeps
// Map<nodeId, Map<paramNumber, { size, value, lastPublish:string }> >
const pendingConfig = new Map();

// Last desired value (for optimistic state)
const lastDesiredConfigState = new Map(); // Map<nodeId, Map<paramNumber, string>>

// ── Helpers ───────────────────────────────────────────────────────────────────
const REGION_TO_ANT = { EU: 'AE', US: 'AU', ANZ: 'AH', JP: 'AH', KR: 'AH' };
const hex16 = (n) => '0x' + Number(n).toString(16).padStart(4, '0');
const awaitMaybe = async (v) => (v && typeof v.then === 'function') ? await v : v;
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

// ---- Supervisor MQTT discovery ---------------------------------------------
const SUPERVISOR_API = 'http://supervisor';
const SUPERVISOR_TOKEN = process.env.SUPERVISOR_TOKEN;

// Try Supervisor -> /services/mqtt first; fall back to env/defaults
async function discoverMqttViaSupervisor() {
  if (!SUPERVISOR_TOKEN) return null;

  // Node 18+ has global fetch. If not, add: import fetch from 'node-fetch';
  const url = `${SUPERVISOR_API}/services/mqtt`;
  try {
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${SUPERVISOR_TOKEN}` },
    });
    if (!res.ok) {
      console.warn('[bridge] Supervisor MQTT discovery failed:', res.status, res.statusText);
      return null;
    }
    const data = await res.json();
    // Expected shape (example):
    // { result: "ok", data: { host, port, username, password, ssl, protocol, service } }
    const svc = data?.data || data; // be tolerant of shape
    if (!svc?.host || !svc?.port) return null;

    const ssl = !!svc.ssl;
    // Build mqtt URL
    const proto = ssl ? 'mqtts' : 'mqtt';
    const urlStr = `${proto}://${svc.host}:${svc.port}`;

    return {
      url: urlStr,
      username: svc.username || undefined,
      password: svc.password || undefined,
      ssl,
      // If Supervisor exposes CA info in the future, wire it here:
      ca: undefined,
      rejectUnauthorized: false, // often self-signed in HA envs
    };
  } catch (e) {
    console.warn('[bridge] Supervisor MQTT discovery error:', e.message);
    return null;
  }
}

function getNode(zw, nodeId) {
  if (!zw) return null;
  return zw.getNode?.(nodeId) || zw.nodes?.[nodeId] || null;
}
function nodeAvailTopic(nodeId){ return `zwave/node/${nodeId}/availability`; }
function isNodeOnline(zw, nodeId) {
  const n = getNode(zw, nodeId);
  return n?.isOnline?.() ?? true; // Homey Node has isOnline()
}

function driverBusyState() {
  const st = zwave?.getState?.() || {};
  return { busy: !!(st.currentProcess || st.currentCommand) };
}

async function awaitDriverIdle({ timeoutMs = 7000, pollMs = 120 } = {}) {
  const start = Date.now();
  while (!shuttingDown) {
    const { busy } = driverBusyState();
    if (!busy) return;
    if (Date.now() - start > timeoutMs) return;
    await sleep(pollMs);
  }
}

async function sendWithRetry(doSend, { maxRetries = 6, baseDelay = 150 } = {}) {
  let delay = baseDelay;
  let lastErr;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    if (shuttingDown) throw new Error('shutting_down');
    await awaitDriverIdle();
    try {
      return await doSend();
    } catch (e) {
      const msg = String(e?.message || e);
      lastErr = e;
      if (/MessageQueue was cleared/i.test(msg) ||
          /not_ready/i.test(msg) ||
          /queue|busy|currentCommand|currentProcess/i.test(msg)) {
        await sleep(delay);
        delay = Math.min(delay * 2, 2000);
        continue;
      }
      throw e;
    }
  }
  throw new Error(`send retry exhausted: ${lastErr?.message || lastErr}`);
}

function normalizeKeyAttr(attr) {
  const s = String(attr || '').toLowerCase().replace(/\s+/g, '');
  if (s.includes('pressed2') || s.includes('double')) return 'double_press';
  if (s.includes('pressed') || s === 'keypressed')   return 'short_press';
  if (s.includes('held') || s === 'keyhelddown')     return 'hold';
  if (s.includes('released'))                        return 'release';
  return 'unknown';
}

// ── Sticky device identity resolution ─────────────────────────────────────────
function isGenericModelName(model) {
  return /^0x[0-9a-f]{4}\/0x[0-9a-f]{4}$/i.test(String(model || ''));
}
function isBetterInfo(current, incoming) {
  const curGeneric = !current || !current.model || isGenericModelName(current.model) || current.manufacturer === 'Z-Wave';
  const incGeneric = !incoming || !incoming.model || isGenericModelName(incoming.model) || incoming.manufacturer === 'Z-Wave';
  if (curGeneric && !incGeneric) return true;         // upgrade
  if (!curGeneric && incGeneric) return false;        // downgrade
  const sameMan = String(current?.manufacturer||'').toLowerCase() === String(incoming?.manufacturer||'').toLowerCase();
  const sameMod = String(current?.model||'').toLowerCase() === String(incoming?.model||'').toLowerCase();
  return !(sameMan && sameMod);
}

async function resolveFromConfigManager(mid, ptype, pid, fwVersion) {
  try {
    const mfName = (await awaitMaybe(cfg.lookupManufacturer?.(mid))) || 'Z-Wave';
    const devCfg = await awaitMaybe(cfg.lookupDevice?.(mid, ptype, pid, fwVersion));
    const model  = devCfg?.label || devCfg?.device?.label || `${hex16(ptype)}/${hex16(pid)}`;
    return { manufacturer: mfName, model, zw_config: devCfg || null };
  } catch {
    return { manufacturer: 'Z-Wave', model: `${hex16(ptype)}/${hex16(pid)}`, zw_config: null };
  }
}

async function ensureResolvedOnce(zw, nodeId) {
  if (nodeResolvePromises.has(nodeId)) return nodeResolvePromises.get(nodeId);
  const p = (async () => {
    const n = getNode(zw, nodeId);
    if (!n) return;
    const mid   = n.manufacturerId?.value ?? n.manufacturerId ?? 0;
    const ptype = n.productTypeId?.value   ?? n.productTypeId   ?? 0;
    const pid   = n.productId?.value       ?? n.productId       ?? 0;
    const appVer = n.applicationVersion ?? n.firmwareVersion ?? undefined;
    const appSub = n.applicationSubVersion ?? undefined;
    const fwVersion = (typeof appVer === 'number' && typeof appSub === 'number') ? `${appVer}.${appSub}`
                    : (typeof appVer === 'string' ? appVer : undefined);
    if (!mid || !ptype || !pid) return;

    const resolved = await resolveFromConfigManager(mid, ptype, pid, fwVersion);
    const base = nodeInfoCache.get(nodeId) || {};
    const next = {
      ...base,
      manufacturer: resolved.manufacturer || base.manufacturer || 'Z-Wave',
      model:        resolved.model || base.model || `${hex16(ptype)}/${hex16(pid)}`,
      ids:          { mid, ptype, pid },
      devCfg:       resolved.zw_config,
      resolved:     true,
    };

    if (!nodeInfoCache.has(nodeId) || isBetterInfo(nodeInfoCache.get(nodeId), next)) {
      nodeInfoCache.set(nodeId, next);
      publishDiscoveryForNode(mqttClient, zw, nodeId, next);
    }

    const defs = paramDefsFromDeviceConfig(next.devCfg);
    if (defs.length) paramDefsCache.set(nodeId, defs);
  })().catch(()=>{});
  nodeResolvePromises.set(nodeId, p);
  return p;
}

function getOrSeedNodeInfo(zw, nodeId) {
  const cached = nodeInfoCache.get(nodeId);
  if (cached) return cached;

  const n = getNode(zw, nodeId);
  const mid   = n?.manufacturerId?.value ?? n?.manufacturerId ?? 0;
  const ptype = n?.productTypeId?.value   ?? n?.productTypeId   ?? 0;
  const pid   = n?.productId?.value       ?? n?.productId       ?? 0;

  const seed = {
    name: n?.name || n?.productDescription || `Node ${nodeId}`,
    manufacturer: n?.manufacturer?.name || n?.manufacturer || 'Z-Wave',
    model: n?.product || `${hex16(ptype)}/${hex16(pid)}`,
    ids: { mid, ptype, pid },
    devCfg: null,
    resolved: false,
  };
  nodeInfoCache.set(nodeId, seed);
  ensureResolvedOnce(zw, nodeId);
  return seed;
}

// ── Param metadata extraction + mapping ───────────────────────────────────────
function paramDefsFromDeviceConfig(devCfg) {
  const defs = [];
  if (!devCfg?.paramInformation) return defs;

  let entries = [];
  const pi = devCfg.paramInformation;
  try {
    if (typeof pi.getAll === 'function') entries = Array.from(pi.getAll());
    else if (typeof pi.entries === 'function') entries = Array.from(pi.entries());
    else if (pi.parameters || pi._parameters) entries = Object.entries(pi.parameters || pi._parameters);
    else if (pi[Symbol.iterator]) entries = Array.from(pi);
  } catch {}

  for (const ent of entries) {
    const key  = Array.isArray(ent) ? ent[0] : ent?.parameter ?? ent?.key;
    const meta = Array.isArray(ent) ? ent[1] : ent?.meta ?? ent;
    const num  = Number(meta?.parameter ?? key?.parameter ?? key);
    if (!Number.isInteger(num) || num <= 0) continue;

    let size  = Number(meta?.valueSize ?? meta?.size);
    const min = (meta?.minValue != null) ? Number(meta.minValue) : undefined;
    const max = (meta?.maxValue != null) ? Number(meta.maxValue) : undefined;

    const label = String(meta?.label || `Parameter ${num}`);
    const desc  = String(meta?.description || '');
    const deflt = (meta?.defaultValue != null) ? meta.defaultValue : undefined;

    let options = [];
    const rawOpts = meta?.options || meta?.states || meta?.values;
    if (rawOpts) {
      if (Array.isArray(rawOpts)) {
        options = rawOpts.map(o => ({
          value: (o.value ?? o.id ?? o.key ?? o.index),
          label: String(o.label ?? o.text ?? o.name ?? o.value)
        })).filter(x => x.value !== undefined);
      } else {
        options = Object.entries(rawOpts).map(([k, v]) =>
          ({ value: Number(k), label: String(v?.label ?? v?.text ?? v ?? k) })
        );
      }
    }

    if (!Number.isInteger(size) || size <= 0) {
      const span = Math.max(Math.abs(min ?? 0), Math.abs(max ?? 0));
      size = (span <= 0x7F ? 1 : span <= 0x7FFF ? 2 : 4);
    }

    defs.push({ num, size, label, description: desc, min, max, options, default: deflt });
  }
  defs.sort((a, b) => a.num - b.num);
  return defs;
}

// strict binary detection
function detectBinary(def) {
  if (!def) return null;
  if (Array.isArray(def.options) && def.options.length === 2) {
    const vals = def.options.map(o => Number(o.value));
    const labels = def.options.map(o => String(o.label || '').toLowerCase());
    const onIdx  = labels.findIndex(l => /(on|enable|enabled|yes|true|sound on|beep on)/i.test(l));
    const offIdx = labels.findIndex(l => /(off|disable|disabled|no|false|sound off|beep off)/i.test(l));
    if (onIdx !== -1 && offIdx !== -1) {
      return { onValue: vals[onIdx], offValue: vals[offIdx] };
    }
    if (new Set(vals).size === 2 && vals.includes(0) && vals.includes(1)) {
      return { onValue: 1, offValue: 0 };
    }
    return null;
  }
  return null;
}

function publishConfigAttributes(nodeId, def) {
  const attrTopic = `zwave/node/${nodeId}/config/${def.num}/attr`;
  const payload = {
    param: def.num, size: def.size, min: def.min, max: def.max, default: def.default,
    onValue: def.onValue, offValue: def.offValue,
    options: def.options?.map(o => ({ value: o.value, label: o.label })),
  };
  mqttClient.publish(attrTopic, JSON.stringify(payload), { retain: true });
}

// Value chooser
function chooseConfigValue(def, msg) {
  const s = String(msg || '').trim();
  if (/^-?\d+$/i.test(s)) return Number(s);

  const u = s.toLowerCase();
  const bin = detectBinary(def);
  if (bin) {
    if (u === 'on')  return Number(bin.onValue);
    if (u === 'off') return Number(bin.offValue);
    return Number(bin.offValue);
  }

  if (def.options?.length) {
    const byLabel = def.options.find(o => String(o.label).toLowerCase() === u);
    if (byLabel) return Number(byLabel.value);

    const findOpt = (re) => def.options.find(o => re.test(String(o.label).toLowerCase()));
    if (/^(on|enable|enabled|yes|true)$/.test(u))  { const o = findOpt(/on|enable|enabled|yes|true/);  if (o) return Number(o.value); }
    if (/^(off|disable|disabled|no|false)$/.test(u)) { const o = findOpt(/off|disable|disabled|no|false/); if (o) return Number(o.value); }

    return Number(def.options[0].value);
  }

  const n = Number(s);
  const raw = Number.isFinite(n) ? n : 0;
  const min = def.min ?? -2147483648;
  const max = def.max ??  2147483647;
  return Math.max(min, Math.min(max, raw));
}

// ── Encoding for Homey sendCommand ────────────────────────────────────────────
function toIntOr(value, fallback=0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}
function intToBytesBE(value, size) {
  let v = toIntOr(value, 0);
  const max = 2 ** (8 * size);
  if (v < 0) v = (max + (v % max)) % max;
  const out = new Uint8Array(size);
  for (let i = size - 1; i >= 0; i--) { out[i] = v & 0xFF; v = Math.floor(v / 256); }
  return Buffer.from(out);
}
function frame(cc, cmd, payload=[]) { return Buffer.from([cc, cmd, ...payload]); }

// CC ids
const CC = { CONFIG: 0x70, SWITCH_BINARY: 0x25, SWITCH_MULTILEVEL: 0x26, BATTERY: 0x80, SUPERVISION: 0x6C };

function nodeSupportsSupervision(zw, nodeId) {
  const n = getNode(zw, nodeId);
  const sup = (n?.commandClassSupported || []).map(x => x.id);
  return sup.includes(108); // 0x6C
}
function nextSupervisionSid(nodeId) {
  const cur = nodeSupervisionSid.get(nodeId) ?? 0;
  const next = (cur + 1) & 0x3F; // 6-bit
  nodeSupervisionSid.set(nodeId, next);
  return next;
}
function encapSupervision(nodeId, innerBuf) {
  const sid = nextSupervisionSid(nodeId);
  return Buffer.concat([ Buffer.from([CC.SUPERVISION, 0x01, sid, 0x00]), innerBuf ]);
}

// Low-level sender (with retries & idle gating). endpointId: omit if 0/undefined
async function sendFrame(nodeId, endpointId, buf) {
  if (typeof zwave?.sendCommand !== 'function') throw new Error('sendCommand not available');
  const ep = (endpointId && endpointId > 0) ? endpointId : undefined;
  return sendWithRetry(() => zwave.sendCommand({ nodeId, endpointId: ep, command: buf }));
}

// High-level CC helpers
async function configSet(zw, nodeId, param, size, value) {
  const vNum = toIntOr(value, 0);
  const raw  = frame(CC.CONFIG, 0x04, [param, size, ...intToBytesBE(vNum, size)]);
  LOG(`[tx] CONFIG_SET n=${nodeId} p=${param} size=${size} val=${vNum} raw=${raw.toString('hex')}`);
  try {
    await sendFrame(nodeId, 0, raw);
  } catch (e) {
    LOG(`configSet raw failed: ${e.message}`);
    throw e;
  }

  if (nodeSupportsSupervision(zw, nodeId)) {
    const sup = encapSupervision(nodeId, raw);
    LOG(`[tx] CONFIG_SET (sup) n=${nodeId} p=${param} val=${vNum} sup=${sup.toString('hex')}`);
    try { await sendFrame(nodeId, 0, sup); } catch (e) { LOG(`configSet sup failed: ${e.message}`); }
  }
}
async function configGet(nodeId, param) {
  const buf = frame(CC.CONFIG, 0x05, [param]);
  LOG(`[tx] CONFIG_GET n=${nodeId} p=${param} ${buf.toString('hex')}`);
  return sendFrame(nodeId, 0, buf);
}
async function switchBinarySet(nodeId, endpointId, on) {
  const buf = frame(CC.SWITCH_BINARY, 0x01, [on ? 0xFF : 0x00]);
  LOG(`[tx] SWITCH_BINARY n=${nodeId} ep=${endpointId} ${buf.toString('hex')}`);
  return sendFrame(nodeId, endpointId, buf);
}
async function multilevelSet(nodeId, endpointId, value) {
  const v = Math.max(0, Math.min(99, Number(value)));
  const buf = frame(CC.SWITCH_MULTILEVEL, 0x01, [v]);
  LOG(`[tx] SWITCH_MULTILEVEL n=${nodeId} ep=${endpointId} ${buf.toString('hex')}`);
  return sendFrame(nodeId, endpointId, buf);
}

// Keep desired/optimistic state
function setLastDesired(nodeId, param, stateStr) {
  if (!lastDesiredConfigState.has(nodeId)) lastDesiredConfigState.set(nodeId, new Map());
  lastDesiredConfigState.get(nodeId).set(param, stateStr);
}
function getLastDesired(nodeId, param) {
  return lastDesiredConfigState.get(nodeId)?.get(param);
}

// Publish optimistic state to HA (so it doesn't bounce UI)
function publishConfigStateOptimistic(nodeId, def, value) {
  const topic = `zwave/node/${nodeId}/config/${def.num}/state`;
  let stateStr = String(value);
  const bin = detectBinary(def);
  if (bin) {
    stateStr = (Number(value) === Number(bin.onValue)) ? 'ON' : 'OFF';
  } else if (def.options?.length) {
    const lbl = (def.options.find(o => String(o.value) === String(value)) || {}).label || String(value);
    stateStr = String(lbl);
  }
  setLastDesired(nodeId, def.num, stateStr);
  mqttClient.publish(topic, stateStr, { retain: true });
}

// Queue write (if sleeping) or send now
async function queueOrSendConfig(nodeId, def, value) {
  if (!isNodeOnline(zwave, nodeId)) {
    if (!pendingConfig.has(nodeId)) pendingConfig.set(nodeId, new Map());
    pendingConfig.get(nodeId).set(def.num, { size: def.size || 1, value, lastPublish: getLastDesired(nodeId, def.num) });
    LOG(`queued CONFIG_SET for sleeping node ${nodeId} p=${def.num} v=${value}`);
    return 'queued';
  }
  await configSet(zwave, nodeId, def.num, def.size || 1, value);
  // schedule confirmation GETs
  setTimeout(() => { configGet(nodeId, def.num).catch(()=>{}); }, 300);
  setTimeout(() => { configGet(nodeId, def.num).catch(()=>{}); }, 2000);
  return 'sent';
}

async function flushPendingForNode(nodeId) {
  const bucket = pendingConfig.get(nodeId);
  if (!bucket || bucket.size === 0) return;
  LOG(`flushing ${bucket.size} queued CONFIG_SET for node ${nodeId}`);
  for (const [param, { size, value }] of bucket.entries()) {
    try {
      await configSet(zwave, nodeId, param, size || 1, value);
      setTimeout(() => { configGet(nodeId, param).catch(()=>{}); }, 300);
    } catch (e) {
      LOG(`flush error node ${nodeId} p=${param}: ${e.message}`);
    }
  }
  bucket.clear();
}

// ── MQTT connect & shutdown ───────────────────────────────────────────────────
async function connectMqtt() {
  // 1) Ask Supervisor
  const sup = await discoverMqttViaSupervisor();

  // 2) Fallbacks (your existing env/defaults)
  const url  = sup?.url  || (process.env.MQTT_URL || 'mqtt://addon_core_mosquitto:1883');
  const user = sup?.username || (process.env.MQTT_USER || 'ha-mqtt-user');
  const pass = sup?.password || (process.env.MQTT_PASSWORD || 'ha-mqtt-user');

  // Client options
  const opts = {
    username: user,
    password: pass,
    reconnectPeriod: 2000,
    clean: true,
    clientId: `homey-zwave-bridge-${Math.random().toString(16).slice(2)}`,
    will: { topic: AVAIL_TOPIC, payload: 'offline', qos: 1, retain: true },
  };

  // TLS handling (Supervisor may report ssl: true)
  if (sup?.ssl) {
    opts.rejectUnauthorized = sup.rejectUnauthorized ?? false;
    if (sup.ca) { opts.ca = sup.ca; }
  }

  const client = mqtt.connect(url, opts);
  client.on('connect', () => LOG(`MQTT connected (${url})`));
  client.on('reconnect', () => LOG('MQTT reconnecting…'));
  client.on('error', (err) => LOG('MQTT error:', err.message));
  client.on('close', () => LOG('MQTT disconnected'));
  return client;
}

function gracefulDown() {
  let done = false;
  const handler = async (sig) => {
    if (done) return;
    done = true; shuttingDown = true;
    LOG(`Caught ${sig}, shutting down…`);
    try { if (zwave?.addNodeAbort) await zwave.addNodeAbort().catch(()=>{}); } catch {}
    try { if (zwave?.removeNodeAbort) await zwave.removeNodeAbort().catch(()=>{}); } catch {}
    try { if (zwave?.inclusion) await zwave.inclusion({ mode: 'ADD_NODE_STOP' }).catch(()=>{}); } catch {}
    try { if (zwave?.exclusion) await zwave.exclusion({ mode: 'REMOVE_NODE_STOP' }).catch(()=>{}); } catch {}
    try { mqttClient?.publish(AVAIL_TOPIC, 'offline', { qos: 1, retain: true }); } catch {}
    try { mqttClient?.end(true); } catch {}
    try { zwave?.destroy?.(); } catch {}
    setTimeout(() => process.exit(0), 300);
  };
  process.on('SIGINT',  () => handler('SIGINT'));
  process.on('SIGTERM', () => handler('SIGTERM'));
}

// ── HA Discovery ─────────────────────────────────────────────────────────────
function devObj(nodeId, info) {
  const id = `zwave_node_${nodeId}`;
  return { ids: [id], name: info?.name || `Z-Wave Node ${nodeId}`, mf: info?.manufacturer || 'Z-Wave', mdl: info?.model || 'Unknown' };
}
function topicSafeId(nodeId, endpoint=0) { return endpoint ? `zwave_node_${nodeId}_ep${endpoint}` : `zwave_node_${nodeId}`; }

function publishControllerDiscovery(client, zw) {
  const base = `${DISC_PREFIX}/sensor/zwave_controller_status`;
  client.publish(`${base}/config`, JSON.stringify({
    name: 'Z-Wave Controller Status',
    uniq_id: 'zwave_controller_status',
    stat_t: `${base}/state`,
    json_attr_t: `${base}/attr`,
    avty_t: AVAIL_TOPIC,
    dev: { ids: ['zwave_controller_homey'], name: 'Homey Z-Wave (bridge)', mf: 'Athom', mdl: 'EFR32ZG14' },
  }), { retain: true });

  client.publish(AVAIL_TOPIC, 'online', { qos: 1, retain: true });
  client.publish(`${base}/state`, 'online', { retain: true });

  const { protocolVersion = {} } = zw.getState?.() || {};
  const { majorVersion, minorVersion, revisionVersion } = protocolVersion || {};
  client.publish(`${base}/attr`, JSON.stringify({
    region: REGION,
    version: `${majorVersion ?? '?'}.${minorVersion ?? '?'}.${revisionVersion ?? '?'}`,
    home_id: `0x${zw.getHomeId?.().toString(16).toUpperCase?.()}`,
  }), { retain: true });

  const ctrlDev = { ids: ['zwave_controller_homey'], name: 'Homey Z-Wave (bridge)', mf: 'Athom', mdl: 'EFR32ZG14' };
  const mkBtn = (key, name, payload) => {
    const t = `${DISC_PREFIX}/button/zwave_${key}`;
    client.publish(`${t}/config`, JSON.stringify({
      name, uniq_id: `zwave_${key}`, cmd_t: CTRL_CMD_TOPIC, pl_prs: payload, entity_category: 'config',
      avty_t: AVAIL_TOPIC, dev: ctrlDev,
    }), { retain: true });
  };
  mkBtn('inclusion_start', 'Z-Wave: Start Inclusion', 'inclusion_start');
  mkBtn('inclusion_stop',  'Z-Wave: Stop Inclusion',  'inclusion_stop');
  mkBtn('exclusion_start', 'Z-Wave: Start Exclusion', 'exclusion_start');
  mkBtn('exclusion_stop',  'Z-Wave: Stop Exclusion',  'exclusion_stop');
}

function publishNodeOnlineDiscovery(client, nodeId, info = {}) {
  const devId = `zwave_node_${nodeId}`;
  const dev = devObj(nodeId, info);
  const base = `${DISC_PREFIX}/binary_sensor/${devId}_online`;
  client.publish(`${base}/config`, JSON.stringify({
    name: `${dev.name} Online`, uniq_id: `${devId}_online`, stat_t: `${base}/state`,
    avty_t: nodeAvailTopic(nodeId), dev,
  }), { retain: true });
  client.publish(nodeAvailTopic(nodeId), 'online', { retain: true });
}

function publishBinarySwitchDiscovery(client, nodeId, endpoint=0, info={}) {
  const devId = topicSafeId(nodeId, endpoint);
  const base  = `${DISC_PREFIX}/switch/${devId}`;
  const state_t  = `zwave/node/${nodeId}${endpoint?`/ep/${endpoint}`:''}/switch_binary/state`;
  const cmd_t    = `zwave/node/${nodeId}${endpoint?`/ep/${endpoint}`:''}/switch_binary/set`;
  client.publish(`${base}/config`, JSON.stringify({
    name: `${info?.name || `Node ${nodeId}`}${endpoint?` EP${endpoint}`:''} Switch`,
    uniq_id: `${devId}_switch`,
    stat_t: state_t,
    cmd_t: cmd_t,
    pl_on: 'ON', pl_off: 'OFF',
    avty_t: nodeAvailTopic(nodeId),
    dev: devObj(nodeId, info),
  }), { retain: true });
}

function publishDimmerDiscovery(client, nodeId, endpoint=0, info={}) {
  const devId = topicSafeId(nodeId, endpoint);
  const base  = `${DISC_PREFIX}/light/${devId}`;
  const st_t  = `zwave/node/${nodeId}${endpoint?`/ep/${endpoint}`:''}/switch_multilevel/state`;
  const cmd_t = `zwave/node/${nodeId}${endpoint?`/ep/${endpoint}`:''}/switch_multilevel/set`;
  const bri_st= `zwave/node/${nodeId}${endpoint?`/ep/${endpoint}`:''}/switch_multilevel/brightness_state`;
  const bri_cm= `zwave/node/${nodeId}${endpoint?`/ep/${endpoint}`:''}/switch_multilevel/brightness_set`;
  client.publish(`${base}/config`, JSON.stringify({
    name: `${info?.name || `Node ${nodeId}`}${endpoint?` EP${endpoint}`:''} Dimmer`,
    uniq_id: `${devId}_light`,
    schema: 'json',
    stat_t: st_t,
    cmd_t: cmd_t,
    brightness_state_topic: bri_st,
    brightness_command_topic: bri_cm,
    brightness_scale: 99,
    avty_t: nodeAvailTopic(nodeId),
    dev: devObj(nodeId, info),
  }), { retain: true });
}

function publishBatteryDiscovery(client, nodeId, info={}) {
  const devId = topicSafeId(nodeId);
  const base  = `${DISC_PREFIX}/sensor/${devId}_battery`;
  const st_t  = `zwave/node/${nodeId}/battery/state`;
  client.publish(`${base}/config`, JSON.stringify({
    name: `${info?.name || `Node ${nodeId}`} Battery`,
    uniq_id: `${devId}_battery`,
    stat_t: st_t,
    unit_of_measurement: '%',
    device_class: 'battery',
    avty_t: nodeAvailTopic(nodeId),
    dev: devObj(nodeId, info),
  }), { retain: true });
}

function publishNotificationBinaryDiscovery(client, nodeId, notificationType, info={}) {
  const devId = topicSafeId(nodeId);
  const base  = `${DISC_PREFIX}/binary_sensor/${devId}_${notificationType}`;
  const st_t  = `zwave/node/${nodeId}/notification/${notificationType}/state`;
  client.publish(`${base}/config`, JSON.stringify({
    name: `${info?.name || `Node ${nodeId}`} ${notificationType}`,
    uniq_id: `${devId}_${notificationType}`,
    stat_t: st_t,
    pl_on: 'ON', pl_off: 'OFF',
    device_class: notificationType,
    avty_t: nodeAvailTopic(nodeId),
    dev: devObj(nodeId, info),
  }), { retain: true });
}

function publishMeterDiscovery(client, nodeId, meterName, unit, devClass, stateClass, info={}) {
  const devId = topicSafeId(nodeId);
  const objId = `${devId}_${meterName}`;
  const base  = `${DISC_PREFIX}/sensor/${objId}`;
  const st_t  = `zwave/node/${nodeId}/meter/${meterName}/state`;
  client.publish(`${base}/config`, JSON.stringify({
    name: `${info?.name || `Node ${nodeId}`} ${meterName}`,
    uniq_id: objId,
    stat_t: st_t,
    unit_of_measurement: unit,
    device_class: devClass || undefined,
    state_class: stateClass || undefined,
    avty_t: nodeAvailTopic(nodeId),
    dev: devObj(nodeId, info),
  }), { retain: true });
}

function publishConfigParamDiscovery(client, nodeId, info, def) {
  const dev = devObj(nodeId, info);
  const devId = topicSafeId(nodeId);
  const baseId = `${devId}_cfg_p${def.num}`;
  const state_t = `zwave/node/${nodeId}/config/${def.num}/state`;
  const cmd_t   = `zwave/node/${nodeId}/config/${def.num}/set`;
  const attr_t  = `zwave/node/${nodeId}/config/${def.num}/attr`;

  const discCommon = {
    name: `${info?.name || `Node ${nodeId}`} - ${def.label}`,
    uniq_id: baseId,
    stat_t: state_t,
    cmd_t: cmd_t,
    json_attr_t: attr_t,
    entity_category: 'config',
    avty_t: nodeAvailTopic(nodeId),
    dev,
  };

  const binaryMeta = detectBinary(def);
  if (binaryMeta) {
    def.onValue  = binaryMeta.onValue;
    def.offValue = binaryMeta.offValue;
    mqttClient.publish(`${DISC_PREFIX}/switch/${baseId}/config`,
      JSON.stringify({ ...discCommon, pl_on: 'ON', pl_off: 'OFF' }),
      { retain: true });
  } else if (def.options && def.options.length && def.options.length <= 50) {
    mqttClient.publish(`${DISC_PREFIX}/select/${baseId}/config`,
      JSON.stringify({ ...discCommon, options: def.options.map(o => String(o.label)) }),
      { retain: true });
  } else {
    mqttClient.publish(`${DISC_PREFIX}/number/${baseId}/config`,
      JSON.stringify({ ...discCommon, min: (def.min ?? 0), max: (def.max ?? 255), step: 1 }),
      { retain: true });
  }

  publishConfigAttributes(nodeId, def);
}

function publishDiscoveryForNode(client, zw, nodeId, info = {}) {
  const n = getNode(zw, nodeId);
  if (!n) return;
  publishNodeOnlineDiscovery(client, nodeId, info);

  const ccSup = (n.commandClassSupported || []).map(x => x.id);
  if (ccSup.includes(37)) publishBinarySwitchDiscovery(client, nodeId, 0, info);
  if (ccSup.includes(38)) publishDimmerDiscovery(client, nodeId, 0, info);
  if (ccSup.includes(128)) publishBatteryDiscovery(client, nodeId, info);
  if (ccSup.includes(113)) ['motion','smoke','door','water','tamper'].forEach(t => publishNotificationBinaryDiscovery(client, nodeId, t, info));
  if (ccSup.includes(50)) {
    publishMeterDiscovery(client, nodeId, 'power',  'W',   'power',  'measurement',      info);
    publishMeterDiscovery(client, nodeId, 'energy', 'kWh', 'energy', 'total_increasing', info);
    publishMeterDiscovery(client, nodeId, 'voltage','V',   'voltage','measurement',      info);
    publishMeterDiscovery(client, nodeId, 'current','A',   'current','measurement',      info);
  }

  // endpoints
  const eps = n._endpoints ? Array.from(n._endpoints.keys()) : [];
  for (const ep of eps) {
    const e = n._endpoints.get(ep);
    if (!e?.commandClassSupported) continue;
    const epCc = e.commandClassSupported.map(x => x.id);
    if (epCc.includes(37)) publishBinarySwitchDiscovery(client, nodeId, ep, info);
    if (epCc.includes(38)) publishDimmerDiscovery(client, nodeId, ep, info);
  }

  // configuration parameters
  const defs = paramDefsCache.get(nodeId) || paramDefsFromDeviceConfig(nodeInfoCache.get(nodeId)?.devCfg);
  if (defs?.length) {
    paramDefsCache.set(nodeId, defs);
    for (const d of defs) publishConfigParamDiscovery(client, nodeId, info, d);
  }
}

// ── Online + Central Scene ────────────────────────────────────────────────────
function publishNodeOnlineState(client, nodeId, online) {
  const base = `${DISC_PREFIX}/binary_sensor/zwave_node_${nodeId}_online`;
  client.publish(`${base}/state`, online ? 'ON' : 'OFF', { retain: true });
  mqttClient?.publish(nodeAvailTopic(nodeId), online ? 'online' : 'offline', { retain: true });
}
function publishCentralSceneEvent(client, nodeId, sceneNum, keyAttr, seq) {
  const action = normalizeKeyAttr(keyAttr);
  const trigTopic = `zwave/node/${nodeId}/central_scene`;
  client.publish(trigTopic, `${sceneNum}:${action}`, { qos: 1 });

  const lastBase = `${DISC_PREFIX}/sensor/zwave_node_${nodeId}_last_action`;
  mqttClient.publish(`${lastBase}/config`, JSON.stringify({
    name: `Node ${nodeId} Last Action`,
    uniq_id: `zwave_node_${nodeId}_last_action`,
    stat_t: `${lastBase}/state`,
    json_attr_t: `${lastBase}/attr`,
    avty_t: nodeAvailTopic(nodeId),
    dev: devObj(nodeId, nodeInfoCache.get(nodeId)),
  }), { retain: true });
  mqttClient.publish(`${lastBase}/state`, `${sceneNum}:${action}`, { retain: true });
  mqttClient.publish(`${lastBase}/attr`, JSON.stringify({ scene: sceneNum, action, seq }), { retain: true });
}

// ── MQTT subscriptions (HA → Z-Wave) ──────────────────────────────────────────
function subscribeDeviceCommands() {
  mqttClient.subscribe('zwave/node/+/switch_binary/set');
  mqttClient.subscribe('zwave/node/+/ep/+/switch_binary/set');
  mqttClient.subscribe('zwave/node/+/switch_multilevel/set');
  mqttClient.subscribe('zwave/node/+/ep/+/switch_multilevel/set');
  mqttClient.subscribe('zwave/node/+/switch_multilevel/brightness_set');
  mqttClient.subscribe('zwave/node/+/ep/+/switch_multilevel/brightness_set');
  mqttClient.subscribe('zwave/node/+/config/+/set');

  mqttClient.on('message', async (topic, payloadBuf) => {
    if (!zwave || shuttingDown) return;
    const msg = (payloadBuf.toString() || '').trim();
    const parts = topic.split('/');

    if (parts[0] !== 'zwave' || parts[1] !== 'node') return;
    const nodeId = Number(parts[2]);
    const hasEp = parts[3] === 'ep';
    const ep = hasEp ? Number(parts[4]) : 0;
    const what = parts.slice(hasEp ? 5 : 3).join('/');

    try {
      if (what === 'switch_binary/set') {
        await switchBinarySet(nodeId, ep, msg.toUpperCase() === 'ON');
      } else if (what === 'switch_multilevel/set') {
        await multilevelSet(nodeId, ep, msg.toUpperCase() === 'ON' ? 99 : 0);
      } else if (what === 'switch_multilevel/brightness_set') {
        await multilevelSet(nodeId, ep, Math.max(0, Math.min(99, Number(msg))));
      } else if (what.startsWith('config/') && what.endsWith('/set')) {
        const pNum = Number(parts[hasEp ? 6 : 4]);
        const defs = paramDefsCache.get(nodeId) || [];
        const def  = defs.find(d => d.num === pNum);
        if (!def) throw new Error(`param ${pNum} not in defs`);

        const value = chooseConfigValue(def, msg);

        // === CONFIG WRITE PATH ===
        // 1) publish optimistic state so HA UI sticks
        publishConfigStateOptimistic(nodeId, def, value);

        // 2) send now, or queue for wake if sleeping
        await queueOrSendConfig(nodeId, def, value);
      }
    } catch (e) {
      if (shuttingDown && /MessageQueue was cleared|shutting_down/i.test(String(e?.message))) return;
      LOG(`config set error (node ${nodeId} ${what}): ${e.message}`);
    }
  });
}

// ── Z-Wave bring-up ──────────────────────────────────────────────────────────
async function initCoprocessorAndZWave() {
  await Coprocessor.getPeripheralLedring().catch(() => {});
  const bridge = await Coprocessor.connect();
  const socket = await bridge.getPeripheralEFR32ZG14();

  const ant = REGION_TO_ANT[REGION];
  if (ant) { try { await socket.switchAntenna(ant); } catch {} }
  try { await socket.reset(); } catch {}

  const settingsPath = "/data/zw_settings.json";
  let zwSettings = {};
  if (fs.existsSync(settingsPath)) { try { zwSettings = JSON.parse(fs.readFileSync(settingsPath)); } catch {} }

  const z = new ZWave({ socket, region: REGION, primary: true, loggingId: 'ha' }, zwSettings);
  z.on('settings', (s) => { try { fs.writeFileSync(settingsPath, JSON.stringify(s)); } catch {} });

  // Wake/online events → flush queued config writes
  z.on('nodeWakeUpStatusChange', (nodeId, online) => {
    publishNodeOnlineState(mqttClient, nodeId, !!online);
    if (online) flushPendingForNode(nodeId).catch(()=>{});
  });

  z.on('state', (st) => {
    try {
      const ids = Array.isArray(st.nodes) ? st.nodes : Object.keys(st.nodes || {}).map(n => Number(n));
      for (const id of ids) {
        const info = getOrSeedNodeInfo(z, id);
        if (!announcedNodes.has(id)) {
          publishDiscoveryForNode(mqttClient, z, id, info);
          announcedNodes.add(id);
        }
        const online = st?.stats?.[`node_${id}_online`] ?? true;
        publishNodeOnlineState(mqttClient, id, !!online);
      }
    } catch (e) { LOG('state handler error:', e.message); }
  });

  z.on('report', (source, report) => {
    try {
      const nodeId = source?.nodeId || source?.id || source;
      if (!nodeId) return;
      const info = nodeInfoCache.get(nodeId) || getOrSeedNodeInfo(z, nodeId);

      if (!announcedNodes.has(nodeId)) {
        publishDiscoveryForNode(mqttClient, z, nodeId, info);
        announcedNodes.add(nodeId);
      }

      const ccName = (report?.commandClassName || report?.commandClass || '').toUpperCase();
      const cmd    = (report?.commandName     || report?.name         || '').toUpperCase();
      const args   = report?.args || report?.data || report || {};
      const ep     = source?.endpoint || 0;

      if (ccName.includes('CENTRAL_SCENE') && /NOTIFICATION|REPORT/.test(cmd)) {
        const scene = args['Scene Number'] ?? args.sceneNumber ?? args.button ?? 0;
        const keyAttr = args['Key Attributes'] ?? args.keyAttributes ?? args.action ?? 'KeyPressed';
        const seq = args['Sequence Number'] ?? args.sequenceNumber ?? null;
        publishCentralSceneEvent(mqttClient, nodeId, scene, keyAttr, seq);
      }

      if (ccName.includes('CONFIGURATION') && /REPORT/.test(cmd)) {
        const param = args['Parameter Number'] ?? args.parameterNumber ?? args.parameter ?? null;
        const value = args['Configuration Value'] ?? args.configurationValue ?? args.value ?? null;
        if (param != null && value != null) {
          const defs = paramDefsCache.get(nodeId) || [];
          const def = defs.find(d => d.num === Number(param));
          const topic = `zwave/node/${nodeId}/config/${param}/state`;
          if (def) {
            const bin = detectBinary(def);
            if (bin) {
              const isOn = Number(value) === Number(bin.onValue);
              mqttClient.publish(topic, isOn ? 'ON' : 'OFF', { retain: true });
            } else if (def.options?.length) {
              const lbl = (def.options.find(o => String(o.value) === String(value)) || {}).label || String(value);
              mqttClient.publish(topic, String(lbl), { retain: true });
            } else {
              mqttClient.publish(topic, String(value), { retain: true });
            }
          } else {
            mqttClient.publish(topic, String(value), { retain: true });
          }
        }
      }

      if (ccName.includes('SWITCH_BINARY')) {
        const isOn = (args.value ?? args.currentValue ?? args.state ?? args.level ?? 0) > 0;
        const stTopic = `zwave/node/${nodeId}${ep?`/ep/${ep}`:''}/switch_binary/state`;
        mqttClient.publish(stTopic, isOn ? 'ON' : 'OFF', { retain: true });
      }
      if (ccName.includes('SWITCH_MULTILEVEL')) {
        const level = Number(args.value ?? args.currentValue ?? args.level ?? 0);
        const on = level > 0;
        const base = `zwave/node/${nodeId}${ep?`/ep/${ep}`:''}/switch_multilevel`;
        mqttClient.publish(`${base}/state`, on ? 'ON' : 'OFF', { retain: true });
        mqttClient.publish(`${base}/brightness_state`, String(level), { retain: true });
      }
      if (ccName.includes('BATTERY')) {
        const pct = Number(args['Battery Level'] ?? args.level ?? args.value ?? 0);
        mqttClient.publish(`zwave/node/${nodeId}/battery/state`, String(pct), { retain: true });
      }
      if (ccName.includes('NOTIFICATION')) {
        const type = String(args['Notification Type'] ?? args.type ?? '').toLowerCase();
        const event = String(args['Event'] ?? args.event ?? '').toLowerCase();
        const map = [
          { key: 'home security', sensor: 'motion', onEvents: ['motion detected','intrusion'] },
          { key: 'smoke alarm',   sensor: 'smoke',  onEvents: ['smoke detected'] },
          { key: 'water alarm',   sensor: 'water',  onEvents: ['water leak detected','water leak'] },
          { key: 'system',        sensor: 'tamper', onEvents: ['tampering detected'] },
          { key: 'access control',sensor: 'door',   onEvents: ['door open','window open'] },
        ];
        for (const m of map) {
          if (type.includes(m.key)) {
            const on = m.onEvents.some(e => event.includes(e));
            const t = `zwave/node/${nodeId}/notification/${m.sensor}/state`;
            mqttClient.publish(t, on ? 'ON' : 'OFF', { retain: true });
          }
        }
      }
      if (ccName.includes('METER')) {
        const scale = (args['Scale'] ?? args.scale ?? '').toString().toLowerCase();
        const value = Number(args['Meter Value'] ?? args.value ?? args.rate ?? 0);
        const pub = (name) => mqttClient.publish(`zwave/node/${nodeId}/meter/${name}/state`, String(value), { retain: true });
        if (scale.includes('w')) pub('power');
        else if (scale.includes('kwh')) pub('energy');
        else if (scale.includes('v')) pub('voltage');
        else if (scale.includes('a')) pub('current');
      }
    } catch (e) { LOG('report handler error:', e.message); }
  });

  z.on('log', (line) => LOG('[zwave]', line));

  await z.init();
  const homeId = z.getHomeId();
  LOG(`Z-Wave ready. HomeID=0x${homeId.toString(16).toUpperCase()} region=${REGION} country=${COUNTRY}`);
  return z;
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  await cfg.loadManufacturers?.();
  await cfg.loadDeviceIndex?.();

  mqttClient = await connectMqtt();
  gracefulDown();

  mqttClient.on('connect', async () => {
    if (zwave) publishControllerDiscovery(mqttClient, zwave);
    mqttClient.subscribe(CTRL_CMD_TOPIC);
    subscribeDeviceCommands();
  });

  mqttClient.on('message', async (topic, payloadBuf) => {
    if (topic !== CTRL_CMD_TOPIC || !zwave || shuttingDown) return;
    const cmd = (payloadBuf.toString() || '').trim().toLowerCase();
    try {
      if (cmd === 'inclusion_start') {
        if (zwave.addNode) zwave.addNode().catch(e => LOG('addNode error:', e.message));
        else await zwave.inclusion({ mode: 'ADD_NODE_ANY' });
      } else if (cmd === 'inclusion_stop') {
        if (zwave.addNodeAbort) await zwave.addNodeAbort();
        else await zwave.inclusion({ mode: 'ADD_NODE_STOP' });
      } else if (cmd === 'exclusion_start') {
        if (zwave.removeNode) zwave.removeNode().catch(e => LOG('removeNode error:', e.message));
        else await zwave.exclusion({ mode: 'REMOVE_NODE_ANY' });
      } else if (cmd === 'exclusion_stop') {
        if (zwave.removeNodeAbort) await zwave.removeNodeAbort();
        else await zwave.exclusion({ mode: 'REMOVE_NODE_STOP' });
      }
    } catch (e) { LOG('command error:', e.message); }
  });

  // Bring up Z-Wave after MQTT
  zwave = await initCoprocessorAndZWave();

  publishControllerDiscovery(mqttClient, zwave);

  // Announce pre-known nodes (seed + resolve once)
  const st = zwave.getState?.() || {};
  const ids = Array.isArray(st.nodes) ? st.nodes : Object.keys(st.nodes || {}).map(n => Number(n));
  for (const id of ids) {
    const info = getOrSeedNodeInfo(zwave, id);
    publishDiscoveryForNode(mqttClient, zwave, id, info);
    announcedNodes.add(id);
    const online = st?.stats?.[`node_${id}_online`] ?? true;
    publishNodeOnlineState(mqttClient, id, !!online);
  }
}

main().catch((err) => {
  console.error('FATAL:', err);
  try { mqttClient?.publish(AVAIL_TOPIC, 'offline', { qos: 1, retain: true }); } catch {}
  process.exit(1);
});