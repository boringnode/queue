import { defineConfig } from 'tsup'

export default defineConfig({
  entry: [
    './index.ts',
    './src/otel.ts',
    './src/types/*.ts',
    './src/drivers/*_adapter.ts',
    './src/contracts/adapter.ts',
  ],
  outDir: './build',
  clean: true,
  format: 'esm',
  dts: true,
  sourcemap: true,
  target: 'esnext',
})
