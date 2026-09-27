#!/usr/bin/env node
/**
 * Run the content-address-web HTTP workbench with durable file storage.
 *   CAW_DIR=./data npm start -- --port 8080
 * On boot the kernel replays events.log from CAW_DIR and resumes exactly
 * where it stopped (upload scratch space under uploads/ survives too).
 */
import { Kernel, FileStorage } from '../src/index.js';
import { createHttpServer } from '../src/http/server.js';

const raw = process.argv.slice(2);
const args = {};
for (let i = 0; i < raw.length; i++) {
  const m = /^--([^=]+)(?:=(.*))?$/.exec(raw[i]);
  if (!m) continue;
  if (m[2] !== undefined) args[m[1]] = m[2];
  else if (raw[i + 1] && !raw[i + 1].startsWith('--')) args[m[1]] = raw[++i];
  else args[m[1]] = true;
}
const dir = process.env.CAW_DIR || args.dir || './data';
const port = Number(process.env.CAW_PORT || args.port || 8080);
const host = process.env.CAW_HOST || args.host || '127.0.0.1';

const storage = await FileStorage.create(dir);
const kernel = await Kernel.recover(storage);
const { url } = await createHttpServer(kernel, { port, host });
console.log(JSON.stringify({
  service: 'content-address-web',
  dataDir: dir,
  url,
  revision: kernel.revision,
  stateHash: kernel.hash(),
}, null, 2));
