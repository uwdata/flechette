import bundleSize from 'rollup-plugin-bundle-size';
import terser from '@rollup/plugin-terser';

export default [
  {
    input: 'src/index.js',
    plugins: [ bundleSize() ],
    output: [
      {
        file: 'dist/flechette.cjs',
        format: 'cjs'
      },
      {
        file: 'dist/flechette.mjs',
        format: 'esm',
      },
      {
        file: 'dist/flechette.min.js',
        format: 'umd',
        sourcemap: true,
        plugins: [ terser({ ecma: 2018 }) ],
        name: 'fl'
      }
    ]
  }
];
