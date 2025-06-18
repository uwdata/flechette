import _duckdb from 'duckdb';

function mergeBuffers(buffers) {
  const len = buffers.reduce((a, b) => a + (b ? b.length : 0), 0);
  const buf = new Uint8Array(len);

  for (let i = 0, offset = 0; i < buffers.length; ++i) {
    if (buffers[i]) {
      buf.set(buffers[i], offset);
      offset += buffers[i].length;
    }
  }

  return buf;
}

export async function duckdb() {
  const db = new _duckdb.Database(':memory:');
  const con = db.connect();

  function exec(sql) {
    return new Promise((resolve, reject) => {
      con.exec(sql, (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      })
    });
  }

  function query(sql) {
    return new Promise((resolve, reject) => {
      this.con.arrowIPCAll(sql, (err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(mergeBuffers(result));
        }
      });
    });
  }

  await exec('INSTALL arrow; LOAD arrow;');

  return { db, con, exec, query };
}
