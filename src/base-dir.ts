import os from "node:os";
import path from "node:path";

let overrideDir: string | undefined;

export function setBaseDir(dir: string): void {
  overrideDir = dir;
}

export function getBaseDir(): string {
  return overrideDir ?? process.env.ACPX_HOME ?? path.join(os.homedir(), ".acpx");
}
