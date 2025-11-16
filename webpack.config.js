const path = require('path');

module.exports = {
  mode: 'production',
  entry: {
    'registrar-numbers': './frontend/registrar-numbers.js',
    'reports': './frontend/reports.js',
  },
  output: {
    filename: '[name].bundle.js',
    path: path.resolve(__dirname, 'app', 'static'),
  },
  performance: {
    maxAssetSize: 1000000,
    maxEntrypointSize: 1000000,
  },
};