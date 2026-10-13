import { PermanentError } from './errors.js';

/** Built-in demo handlers. Real deployments pass their own via `--handlers ./file.js`. */
export const demoHandlers = {
  echo: async (job) => ({ echoed: job.payload }),
  sleep: async (job, { signal }) => {
    const ms = Math.min(Number(job.payload.ms) || 100, 60000);
    await new Promise((res, rej) => {
      const t = setTimeout(res, ms);
      signal?.addEventListener('abort', () => { clearTimeout(t); rej(new Error('aborted')); }, { once: true });
    });
    return { sleptMs: ms };
  },
  fail: async (job) => { throw new Error(String(job.payload.message ?? 'intentional failure')); },
  'fail-permanent': async (job) => { throw new PermanentError(String(job.payload.message ?? 'permanent failure')); },
  flaky: async (job) => {
    const succeedOn = Number(job.payload.succeedOnAttempt) || 3;
    if (job.attempts < succeedOn) throw new Error(`flaky failure on attempt ${job.attempts}`);
    return { attempts: job.attempts };
  },
};
