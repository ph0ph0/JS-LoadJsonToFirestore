const functions = require('firebase-functions')

//UPLOAD RATE IS about 6 products/second

// Create and Deploy Your First Cloud Functions
// https://firebase.google.com/docs/functions/write-firebase-functions

const admin = require('../functions/node_modules/firebase-admin')
const serviceAccount = require("./service-key.json")

// Include the custom logger module we made
const logger = require('./logger')(__filename)

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: "<databaseURL>"
})

// (1) Paste path
const data = require("<JSON containing data to be uploaded>")

// Check if it has "Products" as its root node
if (data.hasOwnProperty("Products")) {
  logger.info('Products is root node')
} else {
  logger.error('{"Products": {}} was not root node')
  process.exitCode(1)
}

/**
 * Data is a collection if
 *  - it has a odd depth
 *  - contains only objects or contains no objects.
 */

// If the data is not an object, equal to null or has no length, then it is not a collection
function isCollection( data, path, depth ) {
  if (
    typeof data != 'object' ||
    data == null ||
    data.length === 0 ||
    isEmpty(data)
  ) {
    return false
  }
  
//if the key is not an object or the key is empty then it is not a collection.
  for (const key in data) {
    if (typeof data[key] != 'object' || data[key] == null) {
      // If there is at least one non-object item then it data then it cannot be collection.
      return false
    }
  }
//otherwise it is a collection
  return true
}

// Checks if object is empty.
function isEmpty(obj) {
  for(const key in obj) {
    if(obj.hasOwnProperty(key)) {
      return false
    }
  }
  return true
}

async function upload(data, path) {
  return await admin.firestore()
    .doc(path.join('/'))
    .set(data)
    .then(() => logger.info(`Document ${path.join('/')} uploaded.`))
    .catch(() => logger.error(`Could not write document ${path.join('/')}.`))
}

/**
 *
 */
async function resolve(data, path = []) {
  if (path.length > 0 && path.length % 2 == 0) {
    // Document's length of path is always even, however, one of keys can actually be a collection.

    // Copy an object.
    const documentData = Object.assign({}, data)

    for (const key in data) {
      // Resolve each collection and remove it from document data.
      if (isCollection(data[key], [...path, key])) {
        // Remove a collection from the document data.
        delete documentData[key]
        // Resolve a colleciton.
        resolve(data[key], [...path, key])
      }
    }

    // If document is empty then it means it only consisted of collections.
    if (!isEmpty(documentData)) {
      // Upload a document free of collections.
      await upload(documentData, path)
    }
  } else {
    // Collection's length of is always odd.
    for (const key in data) {
      // Resolve each collection.
      await resolve(data[key], [...path, key])
    }
  }
}

resolve(data)
