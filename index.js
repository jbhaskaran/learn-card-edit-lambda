const S3 = require('aws-sdk/clients/s3')
const s3 = new S3({
  signatureVersion: 'v4'
})

/*const AWS = require('aws-sdk')
const s3 = new AWS.S3({
  signatureVersion: 'v4',
  region: 'us-west-2',
  credentials: new AWS.SharedIniFileCredentials({ profile: 'mfa' })
})*/

const BUCKET = 'onstartgo.com'
const BUCKET_PREFIX = 'https://s3.us-west-2.amazonaws.com/onstartgo.com/'

const imageDimensions = require('image-size')
const sharp = require('sharp')
const https = require('https')

const getImageUrl = card => {
  if (card.component === 'Image') {
    return card.meta.url || false
  } else {
    return card.meta.imageUrl || false
  }
}

const calcHeight = (newWidth, currentDimensions) => {
  return parseInt(
    (currentDimensions.height * (newWidth / currentDimensions.width)).toFixed(0)
  )
}

const resize = ({ imageBuffer, dimensions, width = 256 }) => {
  const jpegOptions = false
  return new Promise((resolve, reject) => {
    sharp(imageBuffer)
      .resize({
        width,
        height: calcHeight(width, dimensions),
        withoutEnlargement: true
      })
      .jpeg(jpegOptions ? jpegOptions : { force: false })
      .toBuffer((err, data, info) => {
        if (err) {
          reject(err)
        } else {
          resolve(data)
        }
      })
  })
}

const sizeAndUploadImages = async card => {
  const imageUrl = getImageUrl(card)
  if (!imageUrl) {
    return {}
  }
  const keyName = imageUrl.replace(BUCKET_PREFIX, '')
  const imageData = await new Promise((resolve, reject) => {
    https.get(imageUrl, res => {
      let rawData = []
      res.on('data', chunk => {
        rawData.push(chunk)
      })
      res.on('end', async () => {
        try {
          const imageBuffer = Buffer.concat(rawData)
          const dimensions = imageDimensions(imageBuffer)
          const widths = [1024, 512, 256, 128]
          const imageWidths = [dimensions.width]
          for (let index in widths) {
            const width = widths[index]
            if (width <= dimensions.width) {
              const data = await resize({ imageBuffer, dimensions, width })
              imageWidths.push(width)
              const params = {
                Body: data,
                Bucket: BUCKET,
                Key: `${keyName}-${width}`
              }
              await new Promise((resolve, reject) => {
                s3.putObject(params, function(err, data) {
                  if (err) {
                    console.log(err, err.stack)
                    reject(err)
                  } else {
                    resolve(data)
                  }
                })
              })
            }
          }
          resolve({ url: imageUrl, widths: imageWidths })
        } catch (e) {
          console.log(e)
          reject(e)
        }
      })
    })
  })
  console.log(imageData)
  return imageData
}

const path = require('path')
process.env.APP_PATH = path.resolve(
  __dirname,
  'node_modules/@glueit/learn-back/'
)

const storeConfig = require('@glueit/back/src/storeConfig')
const storeInit = require('@glueit/back/src/storeInit')
const store = require('@glueit/back/src/store')

let storeAdapter = null

const entryMap = [
  // latest by user
  { pk: '@user-%userId%', sk: '@updatedTime-%updatedTime%-@cardId-%cardId%' },
  // latest global
  { pk: '@latest', sk: '@updatedTime-%updatedTime%-@cardId-%cardId%' },
  // latest global AND type
  {
    pk: '@latest-@type-%component%',
    sk: '@updatedTime-%updatedTime%-@cardId-%cardId%'
  },
  // latest user AND type
  {
    pk: '@user-%userId%-@type-%component%',
    sk: '@updatedTime-%updatedTime%-@cardId-%cardId%'
  }
  //{ pk: '@share-%cardId%-@user-%userId%', sk: '@updatedTime-%updatedTime%-@cardId-%cardId%' },
  //{ pk: '@share-%cardId%-@user-%userId%-@type-%component%', sk: '@updatedTime-%updatedTime%-@cardId-%cardId%' },
  //{ pk: '@share-%cardId%-@group-%groupId%', sk: '@updatedTime-%updatedTime%-@cardId-%cardId%' },
  //{ pk: '@share-%cardId%-@group-%groupId%-@type-%component%', sk: '@updatedTime-%updatedTime%-@cardId-%cardId%' }
]

const replaceTokens = ({ string, tokens }) => {
  let replacementStr = string
  for (let name in tokens) {
    replacementStr = replacementStr.replace(
      new RegExp(`%${name}%`, 'g'),
      tokens[name]
    )
  }
  return replacementStr
}

exports.handler = async event => {
  const storeName = 'primary'
  const dynamoStoreName = 'cardsBy'
  let isLocal = false
  if (!storeAdapter) {
    const config = await storeConfig(storeName)
    storeAdapter = storeInit(config)
  }

  let messages = []
  if (event.Records) {
    messages = event.Records
  } else {
    isLocal = true
    messages = await store.get({ storeName: 'cardEditQueue', query: {} })
  }
  if (Array.isArray(messages)) {
    for (let i in messages) {
      const message = messages[i]
      const body = JSON.parse(message.body || message.Body)
      const name = body.objectName
      const id = body.id
      const updatedTime = body.updatedTime
      const deleteData = body.deleteData
      const card = await store.get({
        storeName,
        name,
        id,
        idName: 'id',
        query: {}
      })

      const imageData = await sizeAndUploadImages(card)

      const tags = card.tags
      if (card.meta.updatedTime === updatedTime) {
        const cardsByObject = {
          sortKey: null,
          id,
          userId: card.userId,
          cardName: card.name,
          tags: JSON.stringify(card.tags),
          component: card.component,
          cardLevel: card.level,
          imageData: JSON.stringify(imageData)
        }
        const tokens = {
          levelName: card.level,
          updatedTime: card.meta.updatedTime,
          component: card.component,
          userId: card.userId,
          cardId: card.id
        }

        const insertEntry = async ({ tagName, entry }) => {
          if (tagName) {
            tokens.tagName = tagName.toLowerCase()
          }
          cardsByObject.sortKey = replaceTokens({ string: entry.sk, tokens })

          await store.update({
            storeName: dynamoStoreName,
            name,
            id: replaceTokens({ string: entry.pk, tokens }),
            idName: 'partitionKey',
            object: cardsByObject
          })
          delete tokens.tagName
        }
        for (let entry of entryMap) {
          if (/@tag/.test(entry)) {
            for (let tagName of tags) {
              await insertEntry({ tagName, entry })
            }
          } else {
            await insertEntry({ entry })
          }
        }
      }

      if (deleteData !== false) {
        const deleteObject = {
          sortKey: null
        }
        const tokens = {
          levelName: deleteData.level,
          updatedTime: deleteData.prevUpdatedTime,
          component: deleteData.component,
          userId: deleteData.userId
        }

        const deleteEntry = async ({ tagName, entry }) => {
          if (tagName) {
            tokens.tagName = tagName.toLowerCase()
          }
          deleteObject.sortKey = replaceTokens({ string: entry.sk, tokens })
          await store.delete({
            storeName: dynamoStoreName,
            name,
            id: replaceTokens({ string: entry.pk, tokens }),
            idName: 'partitionKey',
            object: deleteObject
          })
          delete tokens.tagName
        }
        for (let entry of entryMap) {
          if (/@tag/.test(entry)) {
            for (let tagName of deleteData.tags) {
              await deleteEntry({ tagName, tokens, entry })
            }
          } else {
            await deleteEntry({ tokens, entry })
          }
        }
      }

      if (isLocal) {
        const object = {
          receiptHandle: message.ReceiptHandle
        }
        await store.delete({ storeName: 'cardEditQueue', object })
      }
    }
  }
}
