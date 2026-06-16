/**
 * fetch-gtfs.js — server-side GTFS-RT decoder for VectorWatch
 * Fetches vehicle positions from open transit APIs, decodes protobuf,
 * writes compact JSON to data/land/{CC}.json for same-origin serving.
 */
'use strict';
const fetch = require('node-fetch');
const protobuf = require('protobufjs');
const zlib = require('zlib');
const fs = require('fs');
const path = require('path');

const OUT_DIR = path.join(__dirname, '../../data/land');
if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

// Full GTFS-RT proto2 schema — includes extension ranges so protobufjs
// can gracefully skip any extension fields (1000-1999) instead of throwing.
const PROTO_SCHEMA = `
syntax = "proto2";
message FeedMessage {
  required FeedHeader header = 1;
  repeated FeedEntity entity = 2;
  extensions 1000 to 1999;
}
message FeedHeader {
  required string gtfs_realtime_version = 1;
  optional uint32 incrementality = 2;
  optional uint64 timestamp = 3;
  extensions 1000 to 1999;
}
message FeedEntity {
  required string id = 1;
  optional bool is_deleted = 2;
  optional TripUpdate trip_update = 3;
  optional VehiclePosition vehicle = 4;
  optional Alert alert = 5;
  extensions 1000 to 1999;
}
message TripUpdate {
  optional TripDescriptor trip = 1;
  optional VehicleDescriptor vehicle = 3;
  extensions 1000 to 1999;
}
message VehiclePosition {
  optional TripDescriptor trip = 1;
  optional VehicleDescriptor vehicle = 8;
  optional Position position = 2;
  optional uint32 current_stop_sequence = 3;
  optional string stop_id = 7;
  optional int32 current_status = 4;
  optional uint64 timestamp = 5;
  optional int32 congestion_level = 6;
  optional int32 occupancy_status = 9;
  extensions 1000 to 1999;
}
message TripDescriptor {
  optional string trip_id = 1;
  optional string route_id = 5;
  optional string direction_id = 6;
  optional string start_time = 2;
  optional string start_date = 3;
  optional int32 schedule_relationship = 4;
  extensions 1000 to 1999;
}
message VehicleDescriptor {
  optional string id = 1;
  optional string label = 2;
  optional string license_plate = 3;
  extensions 1000 to 1999;
}
message Position {
  required float latitude = 1;
  required float longitude = 2;
  optional float bearing = 3;
  optional double odometer = 4;
  optional float speed = 5;
}
message Alert {
  extensions 1000 to 1999;
}
`;

// Feeds — open/free first, keyed ones skipped if secret not set
const FEEDS = [
  {
    code: 'NO',
    url: 'https://api.entur.io/realtime/v1/gtfs-rt/vehicle-positions',
    headers: { 'ET-Client-Name': 'ThorstenGru-VectorWatch' },
  },
  {
    code: 'NL',
    url: 'https://gtfs.ovapi.nl/nl/vehiclePositions.pb',
    headers: {},
  },
  // DE: DELFI free-tier only carries trip_updates; use VBB (Berlin/Brandenburg) for vehicle positions
  {
    code: 'DE',
    url: 'https://realtime.vbb.de/gtfs-rt/v2/vehicle-positions',
    headers: { 'accept': 'application/x-google-protobuf' },
  },
  {
    code: 'SE',
    url: `https://openapi.samtrafiken.se/gtfs-rt-sweden/vehiclepositions.pb?key=${process.env.SE_API_KEY||''}`,
    headers: { 'accept': 'application/x-google-protobuf' },
    requiresKey: 'SE_API_KEY',
  },
  // FR: PRIM v2 endpoint (moved from /marketplace/gtfs-rt/ in 2024)
  {
    code: 'FR',
    url: 'https://prim.iledefrance-mobilites.fr/marketplace/v2/gtfs-realtime/VehiclePosition',
    headers: { 'apikey': process.env.FR_API_KEY||'' },
    requiresKey: 'FR_API_KEY',
  },
  // CH: opentransportdata.swiss vehicle positions endpoint
  {
    code: 'CH',
    url: 'https://api.opentransportdata.swiss/gtfs-rt-fahrzeugpositionen/v1',
    headers: {
      'Authorization': `Bearer ${process.env.CH_API_KEY||''}`,
      'accept': 'application/x-google-protobuf',
    },
    requiresKey: 'CH_API_KEY',
  },
];

// Decompress gzip if magic bytes detected (some servers omit Content-Encoding header)
async function maybeDecompress(buf) {
  if (buf[0] === 0x1f && buf[1] === 0x8b) {
    return new Promise((resolve, reject) =>
      zlib.gunzip(buf, (err, result) => err ? reject(err) : resolve(result))
    );
  }
  return buf;
}

// Patch protobufjs Reader to skip unknown wire types (6,7) and stray end-group (4)
// instead of throwing — some feeds use proprietary proto2 extensions
(function patchReader() {
  const Reader = protobuf.Reader;
  const orig = Reader.prototype.skipType;
  Reader.prototype.skipType = function(wireType) {
    if (wireType === 4) { return this; }          // stray end-group — skip gracefully
    if (wireType === 6) { this.pos += 8; return this; }  // treat as fixed64
    if (wireType === 7) { this.pos += 4; return this; }  // treat as fixed32
    return orig.call(this, wireType);
  };
}());

async function decodeFeed(feed) {
  if (feed.requiresKey && !process.env[feed.requiresKey]) {
    console.log(`[${feed.code}] Skipping — ${feed.requiresKey} secret not set`);
    return null;
  }

  try {
    console.log(`[${feed.code}] Fetching ${feed.url.split('?')[0]}…`);
    const res = await fetch(feed.url, { headers: feed.headers, timeout: 20000 });
    if (!res.ok) {
      console.warn(`[${feed.code}] HTTP ${res.status}`);
      return null;
    }
    const rawBuf = await res.buffer();
    const buf = await maybeDecompress(rawBuf);
    console.log(`[${feed.code}] ${rawBuf.length} bytes raw → ${buf.length} bytes decoded`);

    const root = protobuf.parse(PROTO_SCHEMA, { keepCase: true }).root;
    const FeedMessage = root.lookupType('FeedMessage');

    let msg;
    try {
      msg = FeedMessage.decode(new Uint8Array(buf));
    } catch (e) {
      const hex = Array.from(buf.slice(0, 32)).map(b => b.toString(16).padStart(2,'0')).join(' ');
      const txt = buf.slice(0, 64).toString('utf8').replace(/[^\x20-\x7e]/g, '.');
      console.warn(`[${feed.code}] Decode error: ${e.message}`);
      console.warn(`[${feed.code}] First 32 bytes hex: ${hex}`);
      console.warn(`[${feed.code}] First 64 bytes txt: ${txt}`);
      return null;
    }

    const vehicles = [];
    for (const entity of (msg.entity || [])) {
      try {
        const vp = entity.vehicle;
        if (!vp || !vp.position) continue;
        const p = vp.position;
        if (p.latitude == null || p.longitude == null) continue;
        vehicles.push({
          id: `${feed.code}-${vp.vehicle?.id || vp.vehicleId || entity.id}`,
          lat: p.latitude,
          lng: p.longitude,
          bearing: Math.round(p.bearing || 0),
          speed: Math.round((p.speed || 0) * 3.6),  // m/s → km/h
          line: vp.trip?.route_id || vp.trip?.routeId || '—',
          tripId: vp.trip?.trip_id || vp.trip?.tripId || '—',
          dest: '—',
          routeType: 3,
          label: vp.vehicle?.label || vp.vehicle?.id || entity.id,
          operator: feed.code,
        });
      } catch (e) { /* skip malformed entity */ }
    }

    console.log(`[${feed.code}] ${vehicles.length} vehicles`);
    return { updated: Math.floor(Date.now() / 1000), count: vehicles.length, vehicles };
  } catch (e) {
    console.error(`[${feed.code}] Fatal: ${e.message}`);
    return null;
  }
}

async function main() {
  for (const feed of FEEDS) {
    const result = await decodeFeed(feed);
    if (result && result.vehicles.length > 0) {
      const outPath = path.join(OUT_DIR, `${feed.code}.json`);
      fs.writeFileSync(outPath, JSON.stringify(result));
      console.log(`[${feed.code}] ✓ Wrote ${result.count} vehicles → data/land/${feed.code}.json`);
    }
  }
}

main().catch(e => { console.error(e); process.exit(1); });
