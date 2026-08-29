/*
 * The lab agent phone. Two jobs:
 *
 *   1. Place a real WebRTC call through opensips-agent so one leg of a lab
 *      call is browser media on the agent-side rtpengine.
 *   2. Let every Opus property a browser can vary be varied on purpose --
 *      RED, DTX, VBR/CBR, FEC, stereo, ptime, bitrate, and Chrome's own
 *      AEC/NS/AGC -- so the tap can be tested against each one instead of
 *      against the single friendly shape lab/opus_call_driver.py produces.
 *
 * Every knob is also a URL parameter, so a headless run needs no clicking:
 *   ?autodial=1&seconds=45&codec=opus&dtx=1&red=0&target=4048
 *
 * The "negotiated" panel reports what is actually happening, read from
 * getStats(), not from the SDP. That distinction has already cost this project
 * one wrong conclusion (lab.md's play-media correction): SDP is an intention,
 * getStats is an observation. Packets per second is how you read the effective
 * ptime -- 50/s is 20 ms, 25/s is 40 ms, ~16.7/s is 60 ms -- and it is how DTX
 * announces itself, by dropping toward zero while the mic is silent.
 */

'use strict';

const params = new URLSearchParams(location.search);
const el = (id) => document.getElementById(id);

const DEFAULTS = {
  ws: 'ws://127.0.0.1:5062',
  target: '4048',
  from: 'agent',
  codec: 'opus',
  red: '0',
  dtx: '0',
  fec: '1',
  cbr: '0',
  stereo: '0',
  ptime: '',
  bitrate: '',
  dsp: '0',
  autodial: '0',
  seconds: '0',
  report: '/lab-report',
  register: '1',
  autoanswer: '1',
  source: 'tone',
  tone: '440',
};

const conf = (name) => params.get(name) ?? DEFAULTS[name];
const flag = (name) => ['1', 'true', 'yes', 'on'].includes(String(conf(name)).toLowerCase());

let ua = null;
let session = null;
let statsTimer = null;
let hangupTimer = null;
let previous = { packetsSent: 0, packetsReceived: 0, currentTime: 0, at: 0 };
let reported = false;
let audio = null;
let toneTrack = null;

function log(message) {
  const stamp = new Date().toISOString().slice(11, 23);
  el('log').textContent += `${stamp}  ${message}\n`;
  el('log').scrollTop = el('log').scrollHeight;
  console.log(`[agent] ${message}`);
}

function setState(text) {
  el('state').textContent = text;
  document.title = `agent: ${text}`;
}

/* ---------- SDP surgery ------------------------------------------------- */

function splitSdp(sdp) {
  return sdp.split(/\r\n|\r|\n/).filter((line) => line.length > 0);
}

function joinSdp(lines) {
  return lines.join('\r\n') + '\r\n';
}

function audioSection(lines) {
  const start = lines.findIndex((line) => line.startsWith('m=audio'));
  if (start < 0) return null;
  let end = lines.length;
  for (let i = start + 1; i < lines.length; i += 1) {
    if (lines[i].startsWith('m=')) { end = i; break; }
  }
  return { start, end };
}

function payloadNames(lines, section) {
  const names = new Map();
  for (let i = section.start; i < section.end; i += 1) {
    const match = /^a=rtpmap:(\d+)\s+([^/]+)\//.exec(lines[i]);
    if (match) names.set(match[1], match[2].toLowerCase());
  }
  return names;
}

function payloadFor(lines, section, name) {
  for (const [pt, codec] of payloadNames(lines, section)) {
    if (codec === name) return pt;
  }
  return null;
}

/*
 * Keep only the payload types we want, in the order we want them, and drop
 * every attribute line belonging to the ones we removed. Chrome offers roughly
 * opus, red, G722, PCMU, PCMA, CN and telephone-event; leaving red in the offer
 * is how a tap ends up holding RFC 2198 redundancy blocks instead of Opus
 * frames, so it is opt-in here rather than inherited.
 */
function keepPayloads(sdp, keep) {
  const lines = splitSdp(sdp);
  const section = audioSection(lines);
  if (!section) return sdp;

  const names = payloadNames(lines, section);
  const wanted = [];
  for (const name of keep) {
    for (const [pt, codec] of names) {
      if (codec === name && !wanted.includes(pt)) wanted.push(pt);
    }
  }
  if (wanted.length === 0) {
    log(`sdp: none of ${keep.join(',')} is on offer; leaving the m-line alone`);
    return sdp;
  }

  const header = lines[section.start].split(' ');
  lines[section.start] = header.slice(0, 3).concat(wanted).join(' ');

  const kept = [];
  for (let i = section.start; i < section.end; i += 1) {
    const line = lines[i];
    const owner = /^a=(?:rtpmap|fmtp|rtcp-fb):(\d+)/.exec(line);
    if (owner && !wanted.includes(owner[1])) continue;
    kept.push(line);
  }

  return joinSdp(lines.slice(0, section.start).concat(kept, lines.slice(section.end)));
}

function setFmtp(sdp, pt, overrides) {
  const lines = splitSdp(sdp);
  const index = lines.findIndex((line) => line.startsWith(`a=fmtp:${pt} `));
  const current = new Map();
  if (index >= 0) {
    for (const pair of lines[index].slice(`a=fmtp:${pt} `.length).split(';')) {
      const [key, value] = pair.split('=');
      if (key) current.set(key.trim(), value === undefined ? '' : value.trim());
    }
  }
  for (const [key, value] of Object.entries(overrides)) {
    if (value === null) current.delete(key);
    else current.set(key, String(value));
  }
  const rendered = [...current.entries()]
    .map(([key, value]) => (value === '' ? key : `${key}=${value}`))
    .join(';');
  const line = `a=fmtp:${pt} ${rendered}`;
  if (index >= 0) lines[index] = line;
  else {
    const at = lines.findIndex((l) => l.startsWith(`a=rtpmap:${pt} `));
    lines.splice(at < 0 ? lines.length : at + 1, 0, line);
  }
  return joinSdp(lines);
}

function setPtime(sdp, ms) {
  let lines = splitSdp(sdp).filter((line) => !/^a=(ptime|maxptime):/.test(line));
  const section = audioSection(lines);
  if (!section) return joinSdp(lines);
  lines.splice(section.end, 0, `a=ptime:${ms}`, `a=maxptime:${Math.max(ms, 120)}`);
  return joinSdp(lines);
}

function mungeOffer(sdp) {
  const codec = el('codec').value;
  let out = sdp;

  if (codec !== 'asis') {
    // Only narrow to a codec the far end actually offered. Answering an offer
    // that has no opus with keep=[opus, telephone-event] leaves telephone-event
    // as the only surviving payload type -- an answer carrying DTMF and no
    // audio, which FreeSWITCH rejects as INCOMPATIBLE_DESTINATION. The chosen
    // codec has to be present before anything is stripped.
    const probe = splitSdp(out);
    const where = audioSection(probe);
    const chosen = where ? payloadFor(probe, where, codec) : null;
    if (!chosen) {
      log(`sdp: ${codec} is not in this ${describeAudio(out)}; leaving it untouched`);
      return out;
    }
    const keep = [codec, 'telephone-event'];
    if (codec === 'opus' && el('red').checked) keep.splice(1, 0, 'red');
    out = keepPayloads(out, keep);
  } else if (!el('red').checked) {
    const lines = splitSdp(out);
    const section = audioSection(lines);
    const red = section ? payloadFor(lines, section, 'red') : null;
    if (red) {
      const names = payloadNames(lines, section);
      const keep = [...names.values()].filter((name) => name !== 'red');
      out = keepPayloads(out, [...new Set(keep)]);
    }
  }

  const lines = splitSdp(out);
  const section = audioSection(lines);
  const opus = section ? payloadFor(lines, section, 'opus') : null;
  if (opus) {
    const overrides = {
      useinbandfec: el('fec').checked ? 1 : 0,
      usedtx: el('dtx').checked ? 1 : 0,
    };
    if (el('cbr').checked) overrides.cbr = 1;
    else overrides.cbr = null;
    if (el('stereo').checked) { overrides.stereo = 1; overrides['sprop-stereo'] = 1; }
    const bitrate = el('bitrate').value.trim();
    if (bitrate) overrides.maxaveragebitrate = bitrate;
    out = setFmtp(out, opus, overrides);
  }

  const ptime = el('ptime').value.trim();
  if (ptime) out = setPtime(out, Number(ptime));

  return out;
}

function describeAudio(sdp) {
  if (!sdp) return '—';
  const lines = splitSdp(sdp);
  const section = audioSection(lines);
  if (!section) return 'no audio section';
  const names = payloadNames(lines, section);
  const mline = lines[section.start].split(' ');
  const pts = mline.slice(3);
  const rendered = pts.map((pt) => `${pt}=${names.get(pt) || '?'}`).join(' ');
  const fmtps = lines
    .slice(section.start, section.end)
    .filter((line) => line.startsWith('a=fmtp:'))
    .join('  ');
  const ptime = lines.slice(section.start, section.end).find((l) => l.startsWith('a=ptime:')) || '';
  return `${mline[0]} ${mline[2]} [${rendered}] ${ptime} ${fmtps}`.trim();
}

/* ---------- observation, not assumption -------------------------------- */

async function sampleStats() {
  if (!session || !session.connection) return null;
  const report = await session.connection.getStats();
  const codecs = new Map();
  let outbound = null;
  let inbound = null;

  report.forEach((entry) => {
    if (entry.type === 'codec') codecs.set(entry.id, entry);
    if (entry.type === 'outbound-rtp' && entry.kind === 'audio') outbound = entry;
    if (entry.type === 'inbound-rtp' && entry.kind === 'audio') inbound = entry;
  });

  const now = performance.now();
  const seconds = previous.at ? (now - previous.at) / 1000 : 0;
  const rate = (current, before) => (seconds > 0 ? (current - before) / seconds : 0);

  const describe = (entry) => {
    if (!entry) return '—';
    const codec = codecs.get(entry.codecId);
    const name = codec ? `${codec.mimeType} ${codec.clockRate}Hz ch=${codec.channels || 1}` : 'codec?';
    const fmtp = codec && codec.sdpFmtpLine ? `  ${codec.sdpFmtpLine}` : '';
    return `${name}  pt=${codec ? codec.payloadType : '?'}${fmtp}`;
  };

  const sentRate = outbound ? rate(outbound.packetsSent, previous.packetsSent) : 0;
  const recvRate = inbound ? rate(inbound.packetsReceived, previous.packetsReceived) : 0;

  el('sending').textContent = outbound
    ? `${describe(outbound)}  ${sentRate.toFixed(1)} pkt/s  ${outbound.packetsSent} total`
    : '—';
  // Packets arriving is not audio being heard, and this line used to stop at
  // the packet count. Two more things separate the two: whether the received
  // stream carries any energy at all (silence and comfort noise are packets),
  // and whether the element is actually advancing -- currentTime moving is the
  // only proof playback is running rather than blocked.
  const remote = el('remote');
  const advanced = remote.currentTime - (previous.currentTime || 0);
  const playback = remote.paused
    ? 'PAUSED'
    : advanced > 0
      ? 'playing'
      : 'stalled';
  el('receiving').textContent = inbound
    ? `${describe(inbound)}  ${recvRate.toFixed(1)} pkt/s  ${inbound.packetsReceived} total`
    + `  lost=${inbound.packetsLost ?? '?'}  jitter=${inbound.jitter ?? '?'}`
    + `  level=${(inbound.audioLevel ?? 0).toFixed(4)}`
    + `  energy=${(inbound.totalAudioEnergy ?? 0).toFixed(4)}`
    + `  element=${playback} t=${remote.currentTime.toFixed(1)}s`
    + `  muted=${remote.muted} vol=${remote.volume}`
    : '—';

  previous = {
    currentTime: remote.currentTime,
    packetsSent: outbound ? outbound.packetsSent : 0,
    packetsReceived: inbound ? inbound.packetsReceived : 0,
    at: now,
  };

  return { outbound, inbound, codecs, sentRate, recvRate };
}

/*
 * A headless run has no one to read the panel, so the page hands its summary
 * back as a GET the page server logs. It answers 404 and that is fine -- the
 * access log line is the artifact, and it costs the lab no extra service.
 */
function report(sample, reason) {
  if (reported) return;
  reported = true;
  const url = conf('report');
  if (!url) return;
  const outbound = sample && sample.outbound;
  const inbound = sample && sample.inbound;
  const codec = outbound && sample.codecs.get(outbound.codecId);
  const request = session && session.request;
  const query = new URLSearchParams({
    reason,
    call_id: (request && request.call_id) || '',
    from_tag: (request && request.from && request.from.parameters && request.from.parameters.tag) || '',
    codec: codec ? codec.mimeType : 'none',
    clock: codec ? String(codec.clockRate) : '0',
    fmtp: codec && codec.sdpFmtpLine ? codec.sdpFmtpLine : '',
    sent: outbound ? String(outbound.packetsSent) : '0',
    sent_bytes: outbound ? String(outbound.bytesSent) : '0',
    recv: inbound ? String(inbound.packetsReceived) : '0',
    recv_lost: inbound ? String(inbound.packetsLost ?? -1) : '-1',
    send_rate: sample ? sample.sentRate.toFixed(1) : '0',
    recv_rate: sample ? sample.recvRate.toFixed(1) : '0',
  });
  fetch(`${url}?${query}`, { mode: 'no-cors', cache: 'no-store' }).catch(() => {});
  log(`reported: ${query}`);
}

/* ---------- call control ------------------------------------------------ */

/*
 * Where the outgoing audio comes from.
 *
 * A test tone is the default, and the reason is physical: both endpoints of a
 * lab call run on one machine with one microphone, so a human cannot talk into
 * both ends, and if both capture the same input the recording holds one signal
 * twice -- which is exactly what the first measured run produced. A synthesised
 * tone gives the agent leg an identity nothing else in the call can imitate,
 * needs no microphone permission, and cannot feed back through the speakers.
 *
 * An AudioContext will not start without a user gesture, and an auto-answered
 * call has no gesture, so arm() exists to be clicked once before the call.
 */
function armAudio() {
  if (!audio) {
    audio = new (window.AudioContext || window.webkitAudioContext)();
    log(`audio context created at ${audio.sampleRate} Hz`);
  }
  if (audio.state === 'suspended') audio.resume();
  return audio;
}

function synthStream(hz) {
  const context = armAudio();
  const destination = context.createMediaStreamDestination();
  if (hz > 0) {
    const oscillator = context.createOscillator();
    oscillator.type = 'sine';
    oscillator.frequency.value = hz;
    const gain = context.createGain();
    gain.gain.value = 0.35;
    oscillator.connect(gain);
    gain.connect(destination);
    oscillator.start();
  }
  toneTrack = destination.stream.getAudioTracks()[0];
  log(hz > 0 ? `sending a ${hz} Hz tone, no microphone involved` : 'sending digital silence');
  return destination.stream;
}

async function microphone() {
  const source = el('source').value;
  if (source === 'tone') return synthStream(Number(el('tone').value) || 440);
  if (source === 'silence') return synthStream(0);

  const processing = el('dsp').checked;
  const constraints = {
    audio: {
      echoCancellation: processing,
      noiseSuppression: processing,
      autoGainControl: processing,
    },
    video: false,
  };
  log(`getUserMedia: AEC/NS/AGC ${processing ? 'on (what a real headset does)' : 'off (measurement fidelity)'}`);
  return navigator.mediaDevices.getUserMedia(constraints);
}

function toggleMute() {
  if (!session || !session.connection) return;
  const senders = session.connection.getSenders().filter((s) => s.track);
  if (!senders.length) return;
  const enabled = !senders[0].track.enabled;
  senders.forEach((s) => { s.track.enabled = enabled; });
  el('mute').textContent = enabled ? 'mute' : 'unmute';
  log(enabled ? 'unmuted' : 'muted: the track is still sent, as silence');
}

/*
 * Everything that has to happen to a session whether we placed the call or
 * answered it. The offer/answer munging is the same hook in both directions:
 * on an answer, keepPayloads can only narrow what the far end offered, so a
 * codec the offer never carried is logged and skipped rather than forced.
 */
// Connects the far end's audio to the page's <audio> element, and says so.
//
// Two things here are the fix for a real failure: a browser that completed DTLS
// and received 1596 packets from rtpengine while the person in front of it heard
// nothing.
//
// First, WHEN. JsSIP builds the RTCPeerConnection inside ua.call() and emits
// 'peerconnection' synchronously from it, so handlers attached after the call
// returns have already missed the event -- and with it every 'track' event that
// would ever arrive. On the outgoing path nothing was ever attached to the
// element, which reported itself as paused at t=0.0s while packets piled up in
// the receiver. So this is called from the event AND directly with the
// connection once the call exists, and it sweeps getReceivers() for tracks that
// landed before it got there.
//
// Second, WHETHER. Assigning srcObject is not playing: an <audio autoplay>
// element on a page with no user gesture is blocked and play() rejects, which
// nothing in the page used to report.
function attachRemoteAudio(pc) {
  if (!pc || pc.mssRemoteAttached) return;
  pc.mssRemoteAttached = true;

  const start = (stream, how) => {
    const element = el('remote');
    if (!stream || element.srcObject === stream) return;
    element.srcObject = stream;
    log(`remote track attached (${how})`);
    const started = element.play();
    if (started && typeof started.catch === 'function') {
      started
        .then(() => log(`playback started: muted=${element.muted} volume=${element.volume}`))
        .catch((error) => log(
          `PLAYBACK BLOCKED (${error.name}): ${error.message} `
          + '-- press "arm audio", then dial again'));
    }
  };

  pc.addEventListener('track', (event) => start(event.streams[0], 'track event'));

  const already = pc
    .getReceivers()
    .map((receiver) => receiver.track)
    .filter((track) => track && track.kind === 'audio');
  if (already.length) start(new MediaStream(already), 'receiver already present');
}

function attachSessionHandlers(current) {
  current.on('sdp', (data) => {
    if (data.originator === 'remote') {
      log(`remote ${data.type} received:\n${data.sdp}`);
      return;
    }
    if (data.originator !== 'local') return;
    const before = describeAudio(data.sdp);
    data.sdp = mungeOffer(data.sdp);
    const after = describeAudio(data.sdp);
    el('offered').textContent = after;
    if (before !== after) log(`local ${data.type} munged\n    from  ${before}\n    to    ${after}`);
    else log(`local ${data.type} unchanged: ${after}`);
  });

  current.on('peerconnection', (e) => attachRemoteAudio(e.peerconnection));

  current.on('progress', () => log('180/183 progress'));
  current.on('failed', (e) => {
    setState(`failed: ${e.cause}`);
    log(`call failed: ${e.cause}`);
    finish('failed');
  });
  current.on('ended', (e) => {
    setState('ended');
    log(`call ended by ${e.originator}`);
    finish('ended');
  });
  current.on('confirmed', () => {
    setState('up');
    attachRemoteAudio(current.connection);
    const remote = current.connection.remoteDescription;
    el('answered').textContent = describeAudio(remote ? remote.sdp : '');
    log(`negotiated: ${el('answered').textContent}`);
    log('effective ptime is measured, not read: 50 pkt/s is 20 ms, ~16.7 is 60 ms');
    el('hangup').disabled = false;
    el('mute').disabled = false;
    reported = false;
    previous = { packetsSent: 0, packetsReceived: 0, currentTime: 0, at: 0 };
    if (statsTimer) clearInterval(statsTimer);
    statsTimer = setInterval(sampleStats, 1000);

    const seconds = Number(conf('seconds'));
    if (seconds > 0) {
      log(`hanging up in ${seconds}s (seconds=${seconds})`);
      hangupTimer = setTimeout(() => hangup(), seconds * 1000);
    }
  });
}

async function answerIncoming(incoming) {
  session = incoming;
  setState('incoming');
  log(`INVITE from ${incoming.remote_identity.uri}`);
  attachSessionHandlers(incoming);
  if (!flag('autoanswer')) {
    log('autoanswer is off; nothing will pick this up');
    return;
  }
  let stream;
  try {
    stream = await microphone();
  } catch (error) {
    log(`getUserMedia failed: ${error}; rejecting the call`);
    incoming.terminate({ status_code: 480 });
    return;
  }
  log('answering');
  incoming.answer({
    mediaStream: stream,
    pcConfig: { iceServers: [] },
  });
}

function startUa() {
  if (ua) return ua;
  const registering = flag('register');
  const socket = new JsSIP.WebSocketInterface(el('ws').value.trim());
  ua = new JsSIP.UA({
    sockets: [socket],
    uri: `sip:${el('from').value.trim()}@lab`,
    display_name: 'lab agent',
    register: registering,
    session_timers: false,
  });
  ua.on('connected', () => log(`websocket connected to ${el('ws').value.trim()}`));
  ua.on('disconnected', (e) => log(`websocket disconnected${e && e.error ? ': ' + e.error : ''}`));
  ua.on('registered', () => {
    log('registered: FreeSWITCH can now ring this browser');
    setState('registered');
  });
  ua.on('unregistered', () => log('unregistered'));
  ua.on('registrationFailed', (e) => log(`registration failed: ${e.cause}`));
  ua.on('newRTCSession', (e) => {
    if (e.originator === 'remote') answerIncoming(e.session);
  });
  ua.start();
  log(registering ? 'REGISTER on start (register=1)' : 'not registering (register=0)');
  return ua;
}

async function dial() {
  el('dial').disabled = true;
  setState('dialing');

  let stream;
  try {
    stream = await microphone();
  } catch (error) {
    setState('no microphone');
    log(`getUserMedia failed: ${error}. A non-localhost origin is the usual cause.`);
    el('dial').disabled = false;
    return;
  }

  startUa();
  const target = `sip:${el('target').value.trim()}@lab`;
  log(`INVITE ${target}`);

  session = ua.call(target, {
    mediaStream: stream,
    pcConfig: { iceServers: [] },
    rtcOfferConstraints: { offerToReceiveAudio: 1, offerToReceiveVideo: 0 },
  });

  attachSessionHandlers(session);
  attachRemoteAudio(session.connection);
}

async function finish(reason) {
  if (statsTimer) { clearInterval(statsTimer); statsTimer = null; }
  if (hangupTimer) { clearTimeout(hangupTimer); hangupTimer = null; }
  const sample = await sampleStats().catch(() => null);
  report(sample, reason);
  el('dial').disabled = false;
  el('hangup').disabled = true;
}

function hangup() {
  if (session && !session.isEnded()) {
    log('BYE');
    session.terminate();
  }
}

/* ---------- wiring ------------------------------------------------------ */

for (const name of ['ws', 'target', 'from', 'bitrate', 'tone']) el(name).value = conf(name);
// A <select> whose value is set to something no option carries does not throw
// and does not keep its old value -- it goes empty. So ?codec=PCMU, which looks
// obviously right, silently meant "no codec chosen": the page logged
// codec=PCMU, offered Chrome's full list unchanged, and only worked because MSS
// picks G.711 out of an offer itself. Matching is case-insensitive now, and a
// value that matches nothing says so instead of disappearing.
for (const name of ['codec', 'ptime', 'source']) {
  const wanted = conf(name);
  if (wanted === undefined || wanted === null) continue;
  const options = Array.from(el(name).options).map((option) => option.value);
  const matched = options.find(
    (value) => value.toLowerCase() === String(wanted).toLowerCase());
  if (matched === undefined) {
    log(`${name}=${wanted} is not one of [${options.join(' ')}]; leaving the default`);
    continue;
  }
  el(name).value = matched;
}
for (const name of ['red', 'dtx', 'fec', 'cbr', 'stereo', 'dsp']) el(name).checked = flag(name);

el('arm').addEventListener('click', () => {
  const context = armAudio();
  log(`audio armed, state ${context.state}`);
});
el('mute').addEventListener('click', toggleMute);
el('dial').addEventListener('click', dial);
el('hangup').addEventListener('click', hangup);
el('register').addEventListener('click', () => {
  startUa();
  ua.register();
  log('REGISTER sent (needed only for the FreeSWITCH-dials-the-agent direction)');
});

log(`page loaded. ws=${conf('ws')} target=${conf('target')} codec=${conf('codec')}`);
log(`origin ${location.origin} is ${window.isSecureContext ? 'a secure context' : 'NOT a secure context: the mic will be refused'}`);

// Registering on load is the default, because the interesting direction is
// FreeSWITCH ringing this browser: it cannot do that until the proxy has a
// location entry to look up.
if (flag('register')) startUa();

if (flag('autodial')) {
  log('autodial=1');
  window.addEventListener('load', () => setTimeout(dial, 500));
}
