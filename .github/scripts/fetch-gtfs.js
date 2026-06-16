/**
 * fetch-gtfs.js — server-side GTFS-RT decoder for VectorWatch
 * Fetches vehicle positions from open transit APIs, decodes protobuf,
 * writes compact JSON to data/land/{country}.json for same-origin serving.
 */
const fetch = require('node-fetch');
const protobuf = require('protobufjs');
const fs = require('fs');
const path = require('path');

const OUT_DIR = path.join(__dirname, '../../data/land');
if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

// Minimal GTFS-RT proto schema (vehicle positions only)
const PROTO_SCHEMA = `
syntax = "proto2";
message FeedMessage {
  required FeedHeader header = 1;
  repeated FeedEntity entity = 2;
}
message FeedHeader {
  required string gtfs_realtime_version = 1;
  optional uint64 timestamp = 2;
}
message FeedEntity {
  required string id = 1;
  optional VehiclePosition vehicle = 4;
}
message VehiclePosition {
  optional TripDescriptor trip = 1;
  optional Position position = 2;
  optional string vehicle_id = 3;
  optional VehicleDescriptor vehicle = 8;
}
message TripDescriptor {
  optional string trip_id = 1;
  optional string route_id = 5;
}
message VehicleDescriptor {
  optional string id = 1;
  optional string label = 2;
}
message Position {
  required float latitude = 1;
  required float longitude = 2;
  optional float bearing = 3;
  optional float speed = 4;
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
  {
    code: 'BE',
    url: 'https://gtfs.irail.be/nmbs/realtime/vehicle_positions.pb',
    headers: {},
  },
  {
    code: 'DE',
    url: 'https://realtime.gtfs.de/realtime-free.pb',
    headers: {},
  },
  {
    code: 'SE',
    url: `https://openapi.samtrafiken.se/gtfs-rt-sweden/vehiclepositions.pb?key=${process.env.SE_API_KEY||''}`,
    headers: {},
    requiresKey: 'SE_API_KEY',
  },
  {
    code: 'FR',
    url: 'https://prim.iledefrance-mobilites.fr/marketplace/gtfs-rt/vehicle-positions.pb',
    headers: { 'apikey': process.env.FR_API_KEY||'' },
    requiresKey: 'FR_API_KEY',
  },
  {
    code: 'CH',
    url: 'https://api.opentransportdata.swiss/gtfs-rt2020',
    headers: { 'Authorization': `Bearer ${process.env.CH_API_KEY||''}` },
    requiresKey: 'CH_API_KEY',
  },
];

async function decodeFeed(feed) {
  if (feed.requiresKey && !process.env[feed.requiresKey]) {
    console.log(`[${feed.code}] Skipping — ${feed.requiresKey} secret not set`);
    return null;
  }

  try {
    console.log(`[${feed.code}] Fetching ${feed.url.split('?')[0]}…`);
    const res = await fetch(feed.url, {
      headers: feed.headers,
      timeout: 15000,
    });
    if (!res.ok) {
      console.warn(`[${feed.code}] HTTP ${res.status}`);
      return null;
    }
    const buf = await res.buffer();
    console.log(`[${feed.code}] Got ${buf.length} bytes`);

    const root = protobuf.parse(PROTO_SCHEMA).root;
    const FeedMessage = root.lookupType('FeedMessage');
    const msg = FeedMessage.decode(buf);

    const vehicles = [];
    for (const entity of (msg.entity || [])) {
      const vp = entity.vehicle;
      if (!vp || !vp.position) continue;
      const p = vp.position;
      if (!p.latitude || !p.longitude) continue;
      vehicles.push({
        id: `${feed.code}-${vp.vehicle?.id || vp.vehicleId || entity.id}`,
        lat: p.latitude,
        lng: p.longitude,
        bearing: Math.round(p.bearing || 0),
        speed: Math.round((p.speed || 0) * 3.6),  // m/s → km/h
        line: vp.trip?.routeId || '—',
        tripId: vp.trip?.tripId || '—',
        dest: '—',
        routeType: 3,
        label: vp.vehicle?.label || vp.vehicle?.id || entity.id,
        operator: feed.code,
      });
    }

    console.log(`[${feed.code}] Decoded ${vehicles.length} vehicles`);
    return { updated: Math.floor(Date.now() / 1000), count: vehicles.length, vehicles };
  } catch (e) {
    console.error(`[${feed.code}] Error: ${e.message}`);
    return null;
  }
}

async function main() {
  for (const feed of FEEDS) {
    const result = await decodeFeed(feed);
    if (result && result.vehicles.length > 0) {
      const outPath = path.join(OUT_DIR, `${feed.code}.json`);
      fs.writeFileSync(outPath, JSON.stringify(result));
      console.log(`[${feed.code}] Wrote ${outPath} (${result.count} vehicles)`);
    }
  }
}

main().catch(e => { console.error(e); process.exit(1); });
