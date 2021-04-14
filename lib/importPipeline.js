const logger = require('pelias-logger').get('openaddresses');
const recordStream = require('./streams/recordStream');
const model = require('pelias-model');
const peliasDbclient = require('pelias-dbclient');
const blacklistStream = require('pelias-blacklist-stream');
const isUSorCAHouseNumberZero = require('./streams/isUSorCAHouseNumberZero');

const through = require('through2');
const DUMP_TO = process.env.DUMP_TO;

function createDocumentMapperStream() {
  if(DUMP_TO) {
    return through.obj( function( model, enc, next ){
      next(null, model.callPostProcessingScripts());
    });
  }

  return model.createDocumentMapperStream();
}

/**
 * Import all OpenAddresses CSV files in a directory into Pelias elasticsearch.
 *
 * @param {array of string} files An array of the absolute file-paths to import.
 * @param {object} opts Options to configure the import. Supports the following
 *    keys:
 *
 *      adminValues: Add admin values to each address object (since
 *        OpenAddresses doesn't contain any) using `admin-lookup`. See the
 *        documentation: https://github.com/pelias/admin-lookup
 */
function createFullImportPipeline( files, dirPath, adminLookupStream, importerName ){ // jshint ignore:line
  logger.info( 'Importing %s files.', files.length );

  recordStream.create(files, dirPath)
    .pipe(blacklistStream())
    .pipe(adminLookupStream)
    .pipe(isUSorCAHouseNumberZero.create())
    .pipe(createDocumentMapperStream())
    .pipe(peliasDbclient({name: importerName}));
}

module.exports = {
  create: createFullImportPipeline
};
