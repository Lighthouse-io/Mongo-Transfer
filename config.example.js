export const dynamoTable = 'your-dynamo-table-name'
export const accessKeyId = 'YOUR-ACCESS-KEY-ID'
export const secretAccessKey = 'SECRET-ACCESS-KEY'
export const region = 'us-east-1'
export const sourceDbUrl = 'mongodb://localhost:27017/source_db_name'
export const destinationDbUrl = 'mongodb://localhost:27017/destination_db_name'
export const dynamoTableOpts = {
  key_schema: {
    hash: ['collection', 'string']
  }
}

/**
 * name: mongo collection name
 * sort.field: The field you want to sort data for
 * sort.startValue: The value of the field to start the transfer at
 */
export const collections = [{
  name: 'collectionName',
  sort: {
    startValue: new Date(),
    field: 'created',
  }
}, {
  name: 'events',
  sort: {
    startValue: 'John',
    field: 'name',
  }
}]
