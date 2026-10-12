import { defineConfig } from 'rolldown';

export default defineConfig({
  input: 'src/index.js',
  output: [
    {
      file: 'dist/flechette.cjs',
      format: 'cjs'
    },
    {
      file: 'dist/flechette.mjs',
      format: 'esm'
    },
    {
      file: 'dist/flechette.min.js',
      format: 'umd',
      sourcemap: true,
      minify: true,
      name: 'fl'
    }
  ]
});
