/* eslint-disable @typescript-eslint/no-var-requires */
const path = require("path");
const { ESLINT_MODES } = require("@craco/craco");

module.exports = {
  webpack: {
    configure: (webpackConfig, { _, paths }) => {
      paths.appBuild = webpackConfig.output.path = path.resolve("dist");
      return webpackConfig;
    },
  },
  style: {
    postcssOptions: {
      plugins: [require("tailwindcss"), require("autoprefixer")],
    },
  },
  eslint: {
    mode: ESLINT_MODES.file,
  },
  babel: {
    loaderOptions: {
      ignore: ["./node_modules/mapbox-gl/dist/mapbox-gl.js"],
    },
  },
};
