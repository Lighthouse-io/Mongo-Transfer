import debug from 'debug'
import map from 'lodash/map'
import createDynastyClient from 'dynasty'
import { MongoClient } from 'mongodb'
import {
  series,
  each
} from 'async'

import {
  dynamoTable,
  dynamoTableOpts,
  collections,
  sourceDbUrl,
  destinationDbUrl,
  accessKeyId,
  secretAccessKey,
  region,
} from './config'

const logger = debug('mongo-transfer')

const awsCredentials = {
  accessKeyId,
  secretAccessKey,
  region,
}

const mongoConnections = {}

const dynasty = createDynastyClient(awsCredentials)

series({
  establishTable: done => {

    logger('establish table')

    // check for existing table
    dynasty
      .describe(dynamoTable)
      .then(response => {
        logger('Existing table found')
        return done()
      })
      .error(error => {
        logger('Existing table not found, creating new one...')
        dynasty
          .create(dynamoTable, dynamoTableOpts)
          .then(response => {
            logger('Table created')
            return done()
          })
          .error(done)
      })

  },
  getSourceDb: done => {
    logger('get source connection')
    getMongoClient({
      id: 'source',
      url: sourceDbUrl,
    }, done)
  },
  getDestinationDb: done => {
    logger('get destination connection')
    getMongoClient({
      id: 'destination',
      url: destinationDbUrl,
    }, done)
  },
  processCollectionsCollections: done => {
    each(collections, (collection, next) => {
      transferCollection({
        sourceDb: mongoConnections['source'],
        destinationDb: mongoConnections['destination'],
        tableName: dynamoTable,
        collection
      }, next)
    }, done)
  }
}, function finalCallback(err, result) {
  if (err) {
    logger('Error', err)
    throw err
  }

  logger('** Finished! **')
  process.exit()
})

function transferCollection({
  sourceDb,
  destinationDb,
  tableName,
  collection,
}, callback) {
  const table = dynasty.table(tableName)

  logger('Finding latest document for collection', collection.name)

  table.find(collection.name)
    .then((obj = {}) => {
      logger('found hash key', obj)

      const { value } = obj

      if (!value) {
        logger('no `value` value found for collection')
      }
      // start querying documents for this collection from the start or from _id
      // pulled from database
      transferDocuments({
        sourceDb,
        destinationDb,
        collection,
        startValue: value
      }, callback)

    })
    .error(error => {
      logger('Error', error)
      callback(error)
    })

}

function transferDocuments(opts, callback) {

  let {
    sourceDb,
    destinationDb,
    collection,
    startValue,
    batchSize = 250
  } = opts

  const {
    name,
    sort
  } = collection

  startValue = startValue || sort.startValue

  logger(`
    Transfering Documents for ${name}.
    Starting at ${sort.field}: ${startValue || sort.startValue}
    In Batches of  ${batchSize}
  `)


  let query = {}
  const limit = batchSize

  if (startValue) {
    query[sort.field] = {
      $lt: startValue
    }
  }

  logger(`Finding documents for ${name}`, query)

  findDocuments({
    db: sourceDb,
    collectionName: name,
    query,
    sort: { [sort.field]: -1 },
    limit
  }, (err, documents) => {
    if (err) return callback(err)

    if (documents.length === 0) {
      // handle finish for these documents
      logger(`No more documents found for ${name}`)
      return callback()
    }

    logger(`${documents.length} found to transfer in ${name}`)

    insertDocuments({
      db: destinationDb,
      collectionName: name,
      data: documents
    }, (err, result) => {
      if (err) return callback(err)

      const {
        ops,
        n
      } = result


      const newStartValue = ops[ops.length -1][sort.field]

      // store start value so we can resume transfer if possbile
      const table = dynasty.table(dynamoTable)

      logger(`Updating start value in Dynamo ${name}: ${newStartValue}`)
      table.update(name, {
        value: newStartValue
      }).then(resp => {
        const newOpts = Object.assign({}, opts, {
          startValue: newStartValue
        })
        // recurse to next batch
        return transferDocuments(newOpts, callback)
      })
      .error(error => {
        logger('Error updating start value in Dynamo', error)
        return callback(error)
      })


    })


  })

}

function getMongoClient({
  id, // a reference to cache the connection
  url
}, callback) {

  const db = mongoConnections[id]
  if (db) {
    return callback(null, db)
  }

  logger(`Establishing connection with mongo database ${url} with id ${id}`)

  // create new connection
  return MongoClient.connect(url, (err, db) => {
    if (err) {
      logger('Error establishing mongo connection')
      return callback(err)
    }
    mongoConnections[id] = db
    callback(null, db)
  })
}

function insertDocuments({
  db,
  collectionName,
  data
}, callback) {

  db.collection(collectionName)
    .insertMany(data, (err, result) => {
      if (err) {
        logger(`Error inserting documents into ${collectionName}`)
      } else {
        logger(`Successfully inserted ${data.length} items in to ${collectionName}`)
      }
      callback(err, result)
    })

}

function findDocuments({
  db,
  collectionName,
  query,
  sort,
  limit,
}, callback) {

  db.collection(collectionName)
    .find(query)
    .sort(sort)
    .limit(limit)
    .toArray((err, documents) => {
      if (err) {
        logger('Error finding documents', {
          collectionName,
          query
        })
        return callback(err)
      }

      callback(null, documents)
    })
}
