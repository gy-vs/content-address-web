#!/usr/bin/env node
// 命令行入口：node src/cli.js --data ./.data --port 9090
import { parseArgs } from 'node:util';
import { createServer } from './web/server.js';

const { values } = parseArgs({
  options: {
    data: { type: 'string', default: './.content-address-data' },
    port: { type: 'string', default: '9090' },
    host: { type: 'string', default: '127.0.0.1' },
  },
});

const port = Number(values.port);
const server = await createServer({ dataDir: values.data });
server.listen(port, values.host, () => {
  const info = server.kernel.chainInfo();
  console.log(`content-address-web 监听 http://${values.host}:${port}`);
  console.log(`数据目录: ${values.data} | 链头 seq=${info.headSeq} ${info.headHash}`);
});

const shutdown = async () => {
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 3000).unref();
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
