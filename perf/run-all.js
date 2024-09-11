import { spawn } from 'child_process';

async function node(cmdstr) {
  return new Promise((resolve, reject) => {
    const child = spawn('node', [cmdstr], {
      cwd: process.cwd(),
      detached: true,
      stdio: 'inherit'
    });
    child.on('close', code => {
      if (code !== 0) reject(code);
      resolve();
    });
  });
}

await node('./perf/decode-perf.js');
await node('./perf/encode-perf.js');
await node('./perf/build-perf.js');
