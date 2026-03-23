import { defineConfig } from 'tsup'

export default defineConfig({
  entry: [
    './index.ts',
    './src/otel.ts',
    './src/types/*.ts',
    './src/drivers/*.ts',
    './src/contracts/*.ts',
  ],
  outDir: './build',
  clean: true,
  format: 'esm',
  dts: true,
  sourcemap: true,
  target: 'esnext',
})
