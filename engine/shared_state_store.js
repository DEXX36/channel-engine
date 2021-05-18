const redis = require("redis");
const debug = require("debug")("engine-state-store");
const { performance, PerformanceObserver } = require("perf_hooks");
const sizeof = require("object-sizeof");

const perfObserver = new PerformanceObserver((items) => {
  items.getEntriesByType("measure").forEach((entry) => {
    debug(`${entry.name} ${entry.duration}ms`);
  });
});

const pdebug = (msg) => {
  if (process.env.PERF_LOG) {
    debug(msg);
  }
}

class SharedStateStore {
  constructor(type, opts, initData) {
    this.sharedStates = {};
    this.initData = initData;
    this.keyPrefix = `${type}:`;
    this.type = type;

    this.client = undefined;
    if (opts && opts.redisUrl) {
      debug(`Using REDIS (${opts.redisUrl}) for shared state store (${type})`);
      this.client = redis.createClient(opts.redisUrl);
      if (process.env.PERF_LOG) {
        perfObserver.observe({ entryTypes: ["measure"], buffer: true })
      }
    }
  }

  isShared() {
    return (this.client !== undefined);
  }

  async redisGetAsync(id) {    
    const storeKey = "" + this.keyPrefix + id;
    let readSize;
    const getAsync = new Promise((resolve, reject) => {
      performance.mark("BEGIN READ")
      this.client.get(storeKey, (err, reply) => {
        //debug(`REDIS get ${storeKey}:${reply}`);
        if (!err) {
          performance.mark("END READ")
          performance.mark("BEGIN JSON PARSE");
          readSize = sizeof(reply);
          const json = JSON.parse(reply);
          performance.mark("END JSON PARSE");
          resolve(json);
        } else {
          reject(err);
        }
        });
    });
    const data = await getAsync;
    performance.measure(id + ": redis read: " + Math.floor(readSize/(1024*1024)) + "MiB", "BEGIN READ", "END READ");
    performance.measure(id + ": json parse", "BEGIN JSON PARSE", "END JSON PARSE");
    return data;
  }

  async redisSetAsync(id, data) {
    const storeKey = "" + this.keyPrefix + id;
    let writeSize;
    const setAsync = new Promise((resolve, reject) => {
      performance.mark("BEGIN WRITE")
      const val = JSON.stringify(data);
      writeSize = sizeof(val);
      this.client.set(storeKey, val, (err, res) => {
        //debug(`REDIS set ${storeKey}:${JSON.stringify(data)}`);
        if (!err) {
          performance.mark("END WRITE")
          performance.measure(id + ": redis write: " + Math.floor(writeSize/(1024*1024)) + "MiB", "BEGIN WRITE", "END WRITE");
          resolve(data);
        } else {
          reject(err);
        }
      });
    });
    return await setAsync;
  }

  async init(id) {
    if (!this.client) {
      if (!this.sharedStates[id]) {
        this.sharedStates[id] = {};
        Object.keys(this.initData).forEach(key => {
          this.sharedStates[id][key] = this.initData[key];
        });
      }
      return this.sharedStates[id];
    } else {
      let data = await this.redisGetAsync(id);
      if (data === null) {
        data = await this.redisSetAsync(id, this.initData);
      }
      return data;
    }
  }

  async get(id) {
    let data = this.client ? await this.redisGetAsync(id) : this.sharedStates[id];
    if (!data) {
      data = await this.init(id);
    }
    return data;
  }

  async set(id, key, value) {
    let data = this.client ? await this.redisGetAsync(id) : this.sharedStates[id];
    if (!data) {
      data = await this.init(id);
    }
    data[key] = value;
    if (!this.client) {
      this.sharedStates[id] = data;
    } else {
      await this.redisSetAsync(id, data);
    }
    return data;
  }
}

module.exports = SharedStateStore;