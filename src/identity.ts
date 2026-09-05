import { randomBytes } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

export async function loadIdentity(dataDir: string): Promise<string> {
  const filePath = path.join(dataDir, "identity");
  await mkdir(dataDir, { recursive: true });
  try {
    const value = (await readFile(filePath, "utf8")).trim();
    if (/^[0-9a-f]{32}$/.test(value)) return value;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  const id = randomBytes(16).toString("hex");
  await writeFile(filePath, `${id}\n`, { encoding: "utf8", flag: "wx" }).catch(async (error) => {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
  });
  return (await readFile(filePath, "utf8")).trim();
}
