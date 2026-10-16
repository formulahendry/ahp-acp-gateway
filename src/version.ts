import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

export const VERSION = readPackageVersion();

function readPackageVersion(): string {
  const candidates = [
    new URL('../package.json', import.meta.url),
    new URL('../../package.json', import.meta.url),
  ];
  for (const candidate of candidates) {
    try {
      const raw = JSON.parse(readFileSync(fileURLToPath(candidate), 'utf8')) as {
        version?: unknown;
      };
      if (typeof raw.version === 'string' && raw.version.length > 0) {
        return raw.version;
      }
      throw new Error(`${fileURLToPath(candidate)}: version must be a non-empty string`);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    }
  }
  throw new Error('Unable to locate package.json');
}
